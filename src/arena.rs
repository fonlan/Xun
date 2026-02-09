use std::ops::Range;

#[derive(Default)]
pub struct ByteArena {
    bytes: Vec<u8>,
}

impl ByteArena {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, value: &[u8]) -> SliceRef {
        let start = self.bytes.len() as u32;
        self.bytes.extend_from_slice(value);
        let len = value.len() as u32;
        SliceRef { start, len }
    }

    pub fn get(&self, ptr: SliceRef) -> &[u8] {
        let range = ptr.range();
        &self.bytes[range]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SliceRef {
    pub start: u32,
    pub len: u32,
}

impl SliceRef {
    pub fn range(self) -> Range<usize> {
        let start = self.start as usize;
        let end = start + self.len as usize;
        start..end
    }
}
