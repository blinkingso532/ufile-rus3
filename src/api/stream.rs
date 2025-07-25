use std::{
    pin::Pin,
    sync::{Arc, atomic::AtomicUsize},
    task::Poll,
};

use bytes::Bytes;
use futures::Stream;
use std::fs::File as StdFile;
use tokio::io::{AsyncRead, BufReader};
use tokio::{fs::File, io::ReadBuf};

/// struct to wrap file reader with progress
pub(crate) struct ProgressStream {
    reader: BufReader<File>,
    progress: Arc<AtomicUsize>,
    total_size: usize,
}

impl From<StdFile> for ProgressStream {
    fn from(file: StdFile) -> Self {
        let total_size = file.metadata().unwrap().len() as usize;
        Self {
            reader: BufReader::new(file.into()),
            progress: Arc::new(AtomicUsize::new(0)),
            total_size,
        }
    }
}

impl Stream for ProgressStream {
    type Item = Result<bytes::Bytes, std::io::Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // 8kb buffer
        let mut buffer = [0u8; 8092];
        let mut buf = ReadBuf::new(&mut buffer);
        let this = self.get_mut();
        let reader = Pin::new(&mut this.reader);
        match reader.poll_read(cx, &mut buf) {
            std::task::Poll::Ready(Ok(())) => {
                let bytes = buf.filled().to_vec();
                if bytes.is_empty() {
                    // we are at the end of file.
                    return Poll::Ready(None);
                }
                // read full bytes to the buffer.
                let num_bytes_read = bytes.len();
                let prev = this
                    .progress
                    .fetch_add(num_bytes_read, std::sync::atomic::Ordering::Relaxed);
                let current = prev + num_bytes_read;
                // 计算并打印进度
                let percent = (current as f64 / this.total_size as f64) * 100.0;
                tracing::debug!(
                    "Upload progress: {:.2}% ({} bytes/{} bytes)",
                    percent,
                    current,
                    this.total_size
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
