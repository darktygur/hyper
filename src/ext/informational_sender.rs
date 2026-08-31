//! Support for sending informational responses before a final response.

use http::{Response, StatusCode};

use super::InformationalSender;

/// An error encountered while queuing an informational response.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum InformationalError {
    /// The supplied status is not an informational status, or is `101 Switching Protocols`.
    InvalidStatus,
    /// The client connection is no longer accepting informational responses.
    Closed,
}

impl std::fmt::Display for InformationalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidStatus => write!(f, "response must be 1xx other than 101"),
            Self::Closed => write!(f, "client is no longer accepting informational responses"),
        }
    }
}

impl std::error::Error for InformationalError {}

impl InformationalSender {
    /// Queue an informational response to be sent before the final response.
    ///
    /// `101 Switching Protocols` is excluded because protocol upgrades require
    /// different connection handling than an ordinary informational response.
    pub fn send(&self, response: Response<()>) -> Result<(), InformationalError> {
        if !response.status().is_informational()
            || response.status() == StatusCode::SWITCHING_PROTOCOLS
        {
            return Err(InformationalError::InvalidStatus);
        }

        self.0
            .unbounded_send(response)
            .map_err(|_| InformationalError::Closed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sender() -> (
        InformationalSender,
        futures_channel::mpsc::UnboundedReceiver<Response<()>>,
    ) {
        let (tx, rx) = futures_channel::mpsc::unbounded();
        (InformationalSender(tx), rx)
    }

    #[test]
    fn accepts_informational_responses_except_switching_protocols() {
        let (sender, mut receiver) = sender();

        sender
            .send(Response::builder().status(100).body(()).unwrap())
            .unwrap();
        sender
            .send(Response::builder().status(102).body(()).unwrap())
            .unwrap();
        sender
            .send(Response::builder().status(103).body(()).unwrap())
            .unwrap();

        assert_eq!(receiver.try_recv().unwrap().status(), 100);
        assert_eq!(receiver.try_recv().unwrap().status(), 102);
        assert_eq!(receiver.try_recv().unwrap().status(), 103);
    }

    #[test]
    fn rejects_final_responses_and_switching_protocols() {
        let (sender, _receiver) = sender();

        assert_eq!(
            sender.send(Response::builder().status(101).body(()).unwrap()),
            Err(InformationalError::InvalidStatus)
        );
        assert_eq!(
            sender.send(Response::builder().status(200).body(()).unwrap()),
            Err(InformationalError::InvalidStatus)
        );
    }

    #[test]
    fn reports_a_closed_client() {
        let (sender, receiver) = sender();
        drop(receiver);

        assert_eq!(
            sender.send(Response::builder().status(103).body(()).unwrap()),
            Err(InformationalError::Closed)
        );
    }
}
