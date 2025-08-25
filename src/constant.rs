/// Default multipart size (4MB)
pub(crate) const MULTIPART_SIZE: u32 = 4 << 20;

#[allow(dead_code)]
/// 默认buffer大小（512KB）
pub(crate) const DEFAULT_BUFFER_SIZE: usize = 512 << 10;

/// 默认并发数
pub(crate) const DEFAULT_CONCURRENCY: usize = 8;
