use std::{
    pin::Pin,
    sync::{Arc, atomic::AtomicUsize},
    task::Poll,
};

use bytes::Bytes;
use futures_util::{
    AsyncRead, Stream,
    io::{BufReader, Cursor},
};
use pin_project_lite::pin_project;

pin_project! {
    pub struct ByteStream {
        #[pin]
        inner: Inner
    }
}

struct Inner(bytes::Bytes);

impl ByteStream {
    pub fn from_bytes(bytes: Bytes) -> Self {
        Self {
            inner: Inner(bytes),
        }
    }
}

/// struct to wrap file reader with progress
pub struct ProgressStream<T> {
    reader: BufReader<T>,
    progress: Arc<AtomicUsize>,
    size: usize,
}

impl<T: AsyncRead + Unpin> ProgressStream<T> {
    pub fn new(reader: T, size: usize) -> Self {
        Self {
            reader: BufReader::new(reader),
            progress: Arc::new(AtomicUsize::new(0)),
            size,
        }
    }

    pub fn get_progress(&self) -> usize {
        self.progress.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl<T: AsyncRead + Unpin> Stream for ProgressStream<T> {
    type Item = Result<bytes::Bytes, std::io::Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // 8kb buffer
        let mut buffer = [0u8; 8092];
        let this = self.get_mut();
        let reader = Pin::new(&mut this.reader);
        match reader.poll_read(cx, &mut buffer) {
            std::task::Poll::Ready(Ok(n)) => {
                if n == 0 {
                    // we are at the end of file.
                    return Poll::Ready(None);
                }
                let bytes = buffer[0..n].to_vec();
                let num_bytes_read = bytes.len();
                let prev = this
                    .progress
                    .fetch_add(num_bytes_read, std::sync::atomic::Ordering::Relaxed);
                let current = prev + num_bytes_read;
                // 计算并打印进度
                let percent = (current as f64 / this.size as f64) * 100.0;
                tracing::debug!(
                    "Upload progress: {:.2}% ({} bytes/{} bytes)",
                    percent,
                    current,
                    this.size
                );
                Poll::Ready(Some(Ok(Bytes::from_iter(bytes))))
            }
            std::task::Poll::Ready(Err(error)) => {
                tracing::error!("Failed to read file, error: {:?}", error);
                Poll::Ready(Some(Err(error)))
            }
            std::task::Poll::Pending => Poll::Pending,
        }
    }
}

impl From<ByteStream> for ProgressStream<Cursor<Bytes>> {
    fn from(stream: ByteStream) -> Self {
        let bytes = stream.inner.0;
        let size = bytes.len();
        let cursor = Cursor::new(bytes);
        Self::new(cursor, size)
    }
}
