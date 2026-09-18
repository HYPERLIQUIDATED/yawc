#![cfg(not(target_arch = "wasm32"))]

use std::{
    alloc::{GlobalAlloc, Layout, System},
    hint::black_box,
    sync::atomic::{AtomicUsize, Ordering},
};

use futures::SinkExt;
use yawc::{Frame, Options, Role, WebSocket};

struct Counted;

static LIVE: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: Counted = Counted;

fn allocated(bytes: usize) {
    LIVE.fetch_add(bytes, Ordering::Relaxed);
}

// SAFETY: every operation delegates the original pointer/layout to System;
// counters neither allocate nor change allocation lifetimes.
unsafe impl GlobalAlloc for Counted {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: caller supplies the GlobalAlloc layout contract.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            allocated(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: caller supplies the GlobalAlloc layout contract.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            allocated(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: pointer and layout are forwarded unchanged to their allocator.
        unsafe { System.dealloc(pointer, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: caller supplies a live pointer, matching layout, and valid size.
        let result = unsafe { System.realloc(pointer, layout, size) };
        if !result.is_null() {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            allocated(size);
        }
        result
    }
}

// Keep the allocation check alone in its integration-test process.
#[tokio::test]
async fn control_frames_do_not_allocate_the_sender_but_first_data_send_works() {
    let (client, server) = tokio::io::duplex(32 * 1024);
    let options = Options::default().with_balanced_compression();
    let extension =
        Some("permessage-deflate; server_no_context_takeover; client_no_context_takeover");
    let mut server =
        WebSocket::from_stream_with_extensions(server, Role::Server, extension, options.clone())
            .unwrap();

    let before = LIVE.load(Ordering::Relaxed);
    let mut client =
        WebSocket::from_stream_with_extensions(client, Role::Client, extension, options).unwrap();
    black_box(&client);

    let receive_state = LIVE.load(Ordering::Relaxed).saturating_sub(before);
    assert!(receive_state < 128 * 1024, "receive state: {receive_state}");

    server.send(Frame::ping(vec![1, 2, 3])).await.unwrap();
    assert_eq!(
        client.next_frame().await.unwrap().opcode(),
        yawc::OpCode::Ping
    );
    // Obligated Pong replies are driven by polling the receive side again.
    let pong = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::select! {
            biased;
            unexpected = client.next_frame() => panic!("unexpected client frame: {:?}", unexpected.map(|frame| frame.opcode())),
            frame = server.next_frame() => frame.unwrap(),
        }
    })
    .await
    .unwrap();
    assert_eq!(pong.opcode(), yawc::OpCode::Pong);

    let after_control = LIVE.load(Ordering::Relaxed).saturating_sub(before);
    assert!(after_control < 128 * 1024, "control state: {after_control}");

    for _ in 0..2 {
        client.send(Frame::binary(vec![42; 1024])).await.unwrap();
        assert_eq!(
            server.next_frame().await.unwrap().payload().as_ref(),
            &[42; 1024]
        );
    }

    println!("receive_state_bytes={receive_state} after_control_bytes={after_control}");
}
