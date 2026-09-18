#![cfg(not(target_arch = "wasm32"))]

use futures::SinkExt;
use yawc::{Frame, Options, Role, WebSocket};

// The upstream seeded round-trip test has a watchdog to catch a synchronous spin.
// Pin down its empty-message case through the public WebSocket API as well.
#[tokio::test]
async fn empty_messages_preserve_compression_context() {
    let (client, server) = tokio::io::duplex(32 * 1024);
    let options = Options::default().with_balanced_compression();
    let mut client = WebSocket::from_stream_with_extensions(
        client,
        Role::Client,
        Some("permessage-deflate"),
        options.clone(),
    )
    .unwrap();
    let mut server = WebSocket::from_stream_with_extensions(
        server,
        Role::Server,
        Some("permessage-deflate"),
        options,
    )
    .unwrap();

    for payload in [vec![42; 1952], vec![], vec![], vec![42; 1952]] {
        client.send(Frame::binary(payload.clone())).await.unwrap();
        assert_eq!(
            server.next_frame().await.unwrap().payload().as_ref(),
            payload
        );
    }
}
