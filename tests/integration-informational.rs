#![deny(warnings)]
#![cfg(feature = "http2")]

use bytes::Bytes;
use http_body_util::Full;
use hyper::client::conn::http2::Builder;
use hyper::server::conn::http2::Builder as ServerBuilder;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone)]
struct TokioExecutor;

impl<F> hyper::rt::Executor<F> for TokioExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, future: F) {
        tokio::spawn(future);
    }
}

pin_project! {
    struct TokioIo<T> {
        #[pin]
        inner: T,
    }
}

impl<T> TokioIo<T> {
    fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: tokio::io::AsyncRead> hyper::rt::Read for TokioIo<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let filled = unsafe {
            let mut tokio_buf = tokio::io::ReadBuf::uninit(buf.as_mut());
            match tokio::io::AsyncRead::poll_read(self.project().inner, cx, &mut tokio_buf) {
                Poll::Ready(Ok(())) => tokio_buf.filled().len(),
                other => return other,
            }
        };
        unsafe { buf.advance(filled) };
        Poll::Ready(Ok(()))
    }
}

impl<T: tokio::io::AsyncWrite> hyper::rt::Write for TokioIo<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        tokio::io::AsyncWrite::poll_write(self.project().inner, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        tokio::io::AsyncWrite::poll_flush(self.project().inner, cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        tokio::io::AsyncWrite::poll_shutdown(self.project().inner, cx)
    }

    fn is_write_vectored(&self) -> bool {
        tokio::io::AsyncWrite::is_write_vectored(&self.inner)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<Result<usize, std::io::Error>> {
        tokio::io::AsyncWrite::poll_write_vectored(self.project().inner, cx, bufs)
    }
}

#[tokio::test]
async fn http1_informational_responses_are_forwarded_before_the_final_response() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let service = service_fn(|req: Request<hyper::body::Incoming>| async move {
            let sender = req
                .extensions()
                .get::<hyper::ext::InformationalSender>()
                .expect("informational sender")
                .clone();
            sender
                .send(Response::builder().status(102).body(()).unwrap())
                .unwrap();
            sender
                .send(
                    Response::builder()
                        .status(103)
                        .header("x-hint", "ready")
                        .body(())
                        .unwrap(),
                )
                .unwrap();
            Ok::<_, hyper::Error>(Response::new(Full::new(Bytes::from("done"))))
        });

        hyper::server::conn::http1::Builder::new()
            .serve_connection(TokioIo::new(stream), service)
            .await
            .unwrap();
    });

    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(async move { connection.await.unwrap() });

    let received = Arc::new(Mutex::new(Vec::new()));
    let capture = received.clone();
    let mut request = Request::new(Full::new(Bytes::new()));
    request
        .headers_mut()
        .insert("host", "localhost".parse().unwrap());
    hyper::ext::on_informational(&mut request, move |response| {
        capture
            .lock()
            .unwrap()
            .push((response.status(), response.headers().get("x-hint").cloned()));
    });

    let response = sender.send_request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        *received.lock().unwrap(),
        vec![
            (StatusCode::PROCESSING, None),
            (StatusCode::EARLY_HINTS, Some("ready".parse().unwrap())),
        ]
    );

    server.abort();
}

#[tokio::test]
async fn http2_informational_responses_are_ordered_and_request_scoped() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let service = service_fn(|req: Request<hyper::body::Incoming>| async move {
            let path = req.uri().path().to_owned();
            let sender = req
                .extensions()
                .get::<hyper::ext::InformationalSender>()
                .expect("informational sender")
                .clone();

            if path == "/first" {
                sender
                    .send(Response::builder().status(102).body(()).unwrap())
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(20)).await;
                sender
                    .send(
                        Response::builder()
                            .status(103)
                            .header("x-request", "first")
                            .body(())
                            .unwrap(),
                    )
                    .unwrap();
            } else {
                tokio::time::sleep(Duration::from_millis(5)).await;
                sender
                    .send(
                        Response::builder()
                            .status(103)
                            .header("x-request", "second")
                            .body(())
                            .unwrap(),
                    )
                    .unwrap();
            }

            Ok::<_, hyper::Error>(Response::new(Full::new(Bytes::from(path))))
        });

        ServerBuilder::new(TokioExecutor)
            .serve_connection(TokioIo::new(stream), service)
            .await
            .unwrap();
    });

    let stream = TcpStream::connect(addr).await.unwrap();
    let (sender, connection) = Builder::new(TokioExecutor)
        .handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(async move { connection.await.unwrap() });

    let first_received = Arc::new(Mutex::new(Vec::new()));
    let mut first = Request::new(Full::new(Bytes::new()));
    *first.uri_mut() = "/first".parse().unwrap();
    let first_capture = first_received.clone();
    hyper::ext::on_informational(&mut first, move |response| {
        first_capture.lock().unwrap().push((
            response.status(),
            response.headers().get("x-request").cloned(),
        ));
    });

    let second_received = Arc::new(Mutex::new(Vec::new()));
    let mut second = Request::new(Full::new(Bytes::new()));
    *second.uri_mut() = "/second".parse().unwrap();
    let second_capture = second_received.clone();
    hyper::ext::on_informational(&mut second, move |response| {
        second_capture.lock().unwrap().push((
            response.status(),
            response.headers().get("x-request").cloned(),
        ));
    });

    let first_response = sender.clone().send_request(first);
    let second_response = sender.clone().send_request(second);
    let (first_response, second_response) = tokio::join!(first_response, second_response);

    assert_eq!(first_response.unwrap().status(), StatusCode::OK);
    assert_eq!(second_response.unwrap().status(), StatusCode::OK);
    assert_eq!(
        *first_received.lock().unwrap(),
        vec![
            (StatusCode::PROCESSING, None),
            (StatusCode::EARLY_HINTS, Some("first".parse().unwrap())),
        ]
    );
    assert_eq!(
        *second_received.lock().unwrap(),
        vec![(StatusCode::EARLY_HINTS, Some("second".parse().unwrap()))]
    );

    server.abort();
}
