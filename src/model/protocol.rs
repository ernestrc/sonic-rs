use error::{Result, ErrorKind};
use serde_json::{Value, Map};
use super::*;

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq)]
pub enum MessageKind {
  #[serde(rename="A")]
  AcknowledgeKind,
  #[serde(rename="S")]
  StreamStartedKind,
  #[serde(rename="Q")]
  QueryKind,
  #[serde(rename="H")]
  AuthKind,
  #[serde(rename="T")]
  TypeMetadataKind,
  #[serde(rename="P")]
  ProgressKind,
  #[serde(rename="O")]
  OutputKind,
  #[serde(rename="D")]
  StreamCompletedKind,
}


#[derive(Serialize, Deserialize, Debug)]
pub struct ProtoSonicMessage {
  #[serde(rename="e")]
  pub event_type: MessageKind,
  #[serde(rename="v")]
  pub variation: Option<String>,
  #[serde(rename="p")]
  pub payload: Option<Value>,
}

impl From<SonicMessage> for ProtoSonicMessage {
  fn from(msg: SonicMessage) -> Self {
    match msg {
      SonicMessage::QueryMsg(Query{ config, query, auth, trace_id, .. /* ID is used only internally */ }) => {
        let payload = json!({
          "config": config,
          "auth": auth,
          "trace_id": trace_id
        });

        ProtoSonicMessage {
            event_type: MessageKind::QueryKind,
            variation: Some(query),
            payload: Some(payload),
        }
      },
      SonicMessage::Acknowledge => {
        ProtoSonicMessage {
            event_type: MessageKind::AcknowledgeKind,
            variation: None,
            payload: None,
        }
      },
      SonicMessage::AuthenticateMsg(Authenticate{ user, key, trace_id }) => {
        let payload = json!({
          "user": user,
          "trace_id": trace_id
        });

        ProtoSonicMessage {
            event_type: MessageKind::AuthKind,
            variation: Some(key),
            payload: Some(payload),
        }
      },
      SonicMessage::TypeMetadata(meta) => {
        ProtoSonicMessage {
            event_type: MessageKind::TypeMetadataKind,
            variation: None,
            payload: Some(::serde_json::to_value(&meta).expect("")),
        }
      },
      SonicMessage::QueryProgress{ progress, total, units, status } => {
        let status_v = match status {
            QueryStatus::Queued   => 0,
            QueryStatus::Started  => 1,
            QueryStatus::Running  => 2,
            QueryStatus::Waiting  => 3,
            QueryStatus::Finished => 4
        };

        let payload = json!({
          "p": progress,
          "s": status_v,
          "u": units,
          "t": total
        });

        ProtoSonicMessage {
            event_type: MessageKind::ProgressKind,
            variation: None,
            payload: Some(payload),
        }
      },
      SonicMessage::OutputChunk(data) => {
        ProtoSonicMessage {
            event_type: MessageKind::OutputKind,
            variation: None,
            payload: Some(Value::Array(data)),
        }
      },
      SonicMessage::StreamStarted(trace_id) => {
        ProtoSonicMessage {
            event_type: MessageKind::StreamStartedKind,
            variation: Some(trace_id),
            payload: None,
        }
      },
      SonicMessage::StreamCompleted(res, trace_id) => {
        let payload = json!({
          "trace_id": trace_id,
        });

        ProtoSonicMessage {
            event_type: MessageKind::StreamCompletedKind,
            variation: res,
            payload: Some(payload),
        }
      }
    }
  }
}

fn get_payload(payload: Option<Value>) -> Result<Map<String, Value>> {
  match payload {
    Some(Value::Object(p)) => Ok(p),
    _ => Err(ErrorKind::Proto("msg payload is empty".to_owned()))?,
  }
}

impl ProtoSonicMessage {
  pub fn into_msg(self) -> Result<SonicMessage> {
    let ProtoSonicMessage { event_type, variation, payload } = self;

    trace!("converting ProtoSonicMessage into SonicMessage: {{ event_type: {:?}, variation: \
            {:?}, payload: {:?}}}",
           event_type,
           variation,
           payload);

    match event_type {
      MessageKind::AcknowledgeKind => Ok(SonicMessage::Acknowledge),
      MessageKind::AuthKind => {
        let payload = get_payload(payload)?;
        let key =
          variation.ok_or_else(|| {
              ErrorKind::Proto("AuthenticateMsg: 'variation' field is empty".to_owned())
            })?;
        let user = payload.get("user")
          .and_then(|s| s.as_str().map(|s| s.to_owned()))
          .ok_or_else(|| ErrorKind::Proto("missing 'user' field in payload".to_owned()))?;
        let trace_id = payload.get("trace_id")
          .and_then(|s| s.as_str().map(|s| s.to_owned()));

        Ok(SonicMessage::AuthenticateMsg(Authenticate {
          user: user,
          key: key,
          trace_id: trace_id,
        }))
      }
      MessageKind::TypeMetadataKind => {
        let payload = payload.ok_or_else(|| ErrorKind::Proto("msg payload is empty".to_owned()))?;

        let data = ::serde_json::from_value(payload)?;
        Ok(SonicMessage::TypeMetadata(data))
      }
      MessageKind::ProgressKind => {
        let payload = get_payload(payload)?;

        let total = payload.get("t").and_then(|s| s.as_f64());

        let js = payload.get("s")
          .ok_or_else(|| ErrorKind::Proto("missing 's' field in payload".to_owned()))?;

        let status = match js.as_i64().as_ref().unwrap() {
          &0 => QueryStatus::Queued,
          &1 => QueryStatus::Started,
          &2 => QueryStatus::Running,
          &3 => QueryStatus::Waiting,
          &4 => QueryStatus::Finished,
          s => {
            return Err(ErrorKind::Proto(format!("unexpected query status {:?}", s)).into());
          }
        };

        let progress = payload.get("p")
          .and_then(|s| s.as_f64())
          .ok_or_else(|| ErrorKind::Proto("progress not found in payload".to_owned()))?;

        let units = payload.get("u").and_then(|s| s.as_str().map(|s| s.to_owned()));

        Ok(SonicMessage::QueryProgress {
          progress: progress,
          status: status,
          total: total,
          units: units,
        })
      }
      MessageKind::OutputKind => {
        match payload {
          Some(Value::Array(data)) => Ok(SonicMessage::OutputChunk(data)),
          s => Err(ErrorKind::Proto(format!("payload is not an array: {:?}", s)))?,
        }
      }
      MessageKind::StreamStartedKind => {
        let trace_id = variation.ok_or_else(|| {
                    ErrorKind::Proto("StreamStarted 'variation' field is empty".to_owned())
                })?;

        Ok(SonicMessage::StreamStarted(trace_id))
      }
      MessageKind::StreamCompletedKind => {
        let payload = get_payload(payload)?;

        let trace_id = payload.get("trace_id")
          .and_then(|s| s.as_str().map(|s| s.to_owned()))
          .ok_or_else(|| ErrorKind::Proto("missing 'trace_id' in payload".to_owned()))?;

        Ok(SonicMessage::StreamCompleted(variation, trace_id))
      }
      MessageKind::QueryKind => {
        let payload = get_payload(payload)?;

        let trace_id = payload.get("trace_id")
          .and_then(|t| t.as_str().map(|t| t.to_owned()));

        let auth_token = payload.get("auth")
          .and_then(|a| a.as_str().map(|a| a.to_owned()));

        let query = variation.ok_or_else(|| ErrorKind::Proto("msg variation is empty".to_owned()))?;

        let config = payload.get("config")
          .map(|c| c.to_owned())
          .ok_or_else(|| ErrorKind::Proto("missing 'config' in query message payload".to_owned()))?;

        Ok(SonicMessage::QueryMsg(Query {
          id: None,
          trace_id: trace_id,
          query: query,
          auth: auth_token,
          config: config,
        }))
      }
    }
  }
}
