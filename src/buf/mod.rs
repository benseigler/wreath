use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crossbeam_utils::CachePadded;
mod storage;
use crate::{
    buf::storage::HeapArray,
    reader::{MultiRingReader, RingReader},
    writer::{MultiRingWriter, RingWriter},
};

const INITIAL_POSITION: CachePadded<AtomicUsize> = CachePadded::new(AtomicUsize::new(0));

#[derive(Debug)]
pub struct RingBuf<T> {
    buf: HeapArray<T>,
    pub(crate) write_capacity: usize,
    pub(crate) read_position: CachePadded<AtomicUsize>,
    pub(crate) write_position: CachePadded<AtomicUsize>,
}
impl<T: Copy + Default> RingBuf<T> {
    #[inline]
    fn new(read_capacity: usize, write_capacity: usize) -> (RingReader<T>, RingWriter<T>) {
        let total_length_needed = read_capacity + write_capacity;
        let total_length = power_of_two_ceiling(total_length_needed);
        let buf = HeapArray::<T>::new(total_length);
        let write_capacity = write_capacity + (total_length - total_length_needed);
        let buffer = Arc::new(Self {
            buf,
            write_capacity,
            read_position: INITIAL_POSITION,
            write_position: INITIAL_POSITION,
        });
        (RingReader::new(buffer.clone()), RingWriter::new(buffer))
    }
    #[inline]
    pub fn read_position(&self) -> usize {
        self.read_position.load(Ordering::Acquire)
    }
    #[inline]
    pub fn write_position(&self) -> usize {
        self.write_position.load(Ordering::Acquire)
    }
    #[inline]
    pub fn read_at_index(&self, index: usize) -> T {
        self.buf.read_at_index(index)
    }
    #[inline]
    pub fn write_at_index(&self, index: usize, value: T) {
        self.buf.write_at_index(index, value);
    }
    #[inline]
    pub fn mutate_at_index(&self, index: usize) -> &mut T {
        self.buf.mutate_at_index(index)
    }
    #[inline]
    pub fn advance_read_position_by(&self, amount: usize) -> usize {
        let old = self.read_position.fetch_add(amount, Ordering::AcqRel);
        old + amount
    }
    #[inline]
    pub fn advance_write_position_by(&self, amount: usize) -> usize {
        let old = self.write_position.fetch_add(amount, Ordering::AcqRel);
        old + amount
    }
    #[inline]
    pub fn read_capacity(&self) -> usize {
        self.buf.len() - self.write_capacity()
    }
    #[inline]
    pub fn write_capacity(&self) -> usize {
        self.write_capacity
    }
}

#[derive(Debug)]
pub struct MultiRingBuf<T> {
    buf: HeapArray<T>,
    pub(crate) channels: usize,
    pub(crate) channel_len: usize,
    pub(crate) write_capacity: usize,
    pub(crate) read_position: CachePadded<AtomicUsize>,
    pub(crate) write_position: CachePadded<AtomicUsize>,
}

impl<T: Default + Copy> MultiRingBuf<T> {
    #[inline]
    pub fn new(
        channels: usize,
        read_capacity: usize,
        write_capacity: usize,
    ) -> (MultiRingReader<T>, MultiRingWriter<T>) {
        let channel_len_needed = read_capacity + write_capacity;
        let total_length_needed = channel_len_needed * channels;
        let total_length = power_of_two_ceiling(total_length_needed);
        let write_capacity = write_capacity + ((total_length - total_length_needed) / channels);
        let channel_len = read_capacity + write_capacity;
        let buf = HeapArray::new(total_length);
        let ringbuf = Arc::new(Self {
            channels,
            channel_len,
            write_capacity,
            read_position: INITIAL_POSITION,
            write_position: INITIAL_POSITION,
            buf,
        });
        (
            MultiRingReader::new(ringbuf.clone()),
            MultiRingWriter::new(ringbuf),
        )
    }
    #[inline]
    pub fn read_position(&self) -> usize {
        self.read_position.load(Ordering::Acquire)
    }
    #[inline]
    pub fn write_position(&self) -> usize {
        self.write_position.load(Ordering::Acquire)
    }
    #[inline]
    pub fn read_at_index(&self, channel: usize, index: usize) -> T {
        self.buf
            .multi_read_at_index(self.channel_len, channel, index)
    }
    #[inline]
    pub fn write_at_index(&self, channel: usize, index: usize, value: T) {
        self.buf
            .multi_write_at_index(self.channel_len, channel, index, value)
    }
    #[inline]
    pub fn mutate_at_index(&self, channel: usize, index: usize) -> &mut T {
        self.buf
            .multi_mutate_at_index(self.channel_len, channel, index)
    }
    #[inline]
    pub fn advance_read_position_by(&self, amount: usize) -> usize {
        let old = self.read_position.fetch_add(amount, Ordering::AcqRel);
        old + amount
    }
    #[inline]
    pub fn advance_write_position_by(&self, amount: usize) -> usize {
        let old = self.write_position.fetch_add(amount, Ordering::AcqRel);
        old + amount
    }
    #[inline]
    pub fn read_capacity(&self) -> usize {
        self.channel_len - self.write_capacity
    }
    #[inline]
    pub fn write_capacity(&self) -> usize {
        self.write_capacity
    }
    #[inline]
    pub fn channels(&self) -> usize {
        self.channels
    }
}

/// Creates a single-channel ring buffer and returns a reader/writer pair
#[inline]
pub fn ring_buf<T>(read_capacity: usize, write_capacity: usize) -> (RingReader<T>, RingWriter<T>)
where
    T: Copy + Default,
{
    RingBuf::<T>::new(read_capacity, write_capacity)
}

/// Creates a multi-channel ring buffer and returns a reader/writer pair
#[inline]
pub fn multi_ring_buf<T>(
    channels: usize,
    read_capacity: usize,
    write_capacity: usize,
) -> (MultiRingReader<T>, MultiRingWriter<T>)
where
    T: Copy + Default,
{
    MultiRingBuf::<T>::new(channels, read_capacity, write_capacity)
}

#[inline]
fn power_of_two_ceiling(value: usize) -> usize {
    for i in 0..usize::BITS {
        let pow = 1 << i;
        if pow >= value {
            return pow;
        }
    }
    0
}
