#![cfg(not(target_arch = "wasm32"))]

use flate2::{Compress, Compression, FlushCompress};
use tokio::io::{duplex, AsyncWriteExt};
use yawc::{Options, Role, WebSocket};

fn encode(compressor: &mut Compress, input: &[u8]) -> Vec<u8> {
    let mut output = vec![0; input.len() * 2 + 128];
    let before_in = compressor.total_in();
    let before_out = compressor.total_out();

    compressor
        .compress(input, &mut output, FlushCompress::Sync)
        .unwrap();
    assert_eq!(
        compressor.total_in() - before_in,
        u64::try_from(input.len()).unwrap()
    );
    output.truncate(usize::try_from(compressor.total_out() - before_out).unwrap());
    assert!(output.ends_with(&[0, 0, 255, 255]));
    output.truncate(output.len() - 4);
    output
}

fn append_frame(packet: &mut Vec<u8>, opcode: u8, payload: &[u8]) {
    packet.push(opcode);

    if payload.len() < 126 {
        packet.push(u8::try_from(payload.len()).unwrap());
    } else {
        packet.push(126);
        packet.extend_from_slice(&u16::try_from(payload.len()).unwrap().to_be_bytes());
    }

    packet.extend_from_slice(payload);
}

#[tokio::test]
async fn decoded_limit_spans_fragments_and_resets_between_messages() {
    for no_context_takeover in [false, true] {
        for fragmented in [false, true] {
            let extension = if no_context_takeover {
                "permessage-deflate; server_no_context_takeover; client_no_context_takeover"
            } else {
                "permessage-deflate"
            };
            let (client, mut peer) = duplex(16 * 1024);
            let mut ws = WebSocket::from_stream_with_extensions(
                client,
                Role::Client,
                Some(extension),
                Options::default()
                    .with_balanced_compression()
                    .with_max_read_buffer(1025),
            )
            .unwrap();
            let mut compressor = Compress::new(Compression::default(), false);

            // Multiple exact-limit messages verify budget reset independently of
            // compression dictionary reset. The final message exceeds by one byte.
            for size in [0, 1024, 1024, 1025] {
                let input = vec![42; size];
                let compressed = encode(&mut compressor, &input);

                if no_context_takeover {
                    compressor.reset();
                }

                let mut packet = Vec::new();

                if fragmented {
                    let middle = compressed.len().div_ceil(2);
                    append_frame(&mut packet, 0x42, &compressed[..middle]);
                    append_frame(&mut packet, 0x89, &[]);
                    append_frame(&mut packet, 0x80, &compressed[middle..]);
                } else {
                    append_frame(&mut packet, 0xc2, &compressed);
                }

                peer.write_all(&packet).await.unwrap();

                let result = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                    loop {
                        match ws.next_frame().await {
                            Ok(frame) if frame.opcode().is_control() => {}
                            result => break result,
                        }
                    }
                })
                .await
                .expect("frame read must terminate");

                if size <= 1024 {
                    assert_eq!(result.unwrap().payload().as_ref(), input);
                } else {
                    let error = result.err().expect("oversized decoded message must fail");
                    assert!(error.to_string().contains("exceeds limit"), "{error}");
                }
            }
        }
    }
}
