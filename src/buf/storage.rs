use std::mem::ManuallyDrop;

#[derive(Debug)]
pub(crate) struct HeapArray<T> {
    ptr: *mut T,
    len: usize,
}

impl<T: Default + Copy> HeapArray<T> {
    #[inline]
    pub fn new(len: usize) -> Self {
        let ptr = ManuallyDrop::new(vec![T::default(); len].into_boxed_slice()).as_mut_ptr();
        Self { ptr, len }
    }
}

impl<T> HeapArray<T> {
    #[inline]
    fn mut_ptr(&self) -> *mut T {
        self.ptr
    }
    #[inline]
    fn const_ptr(&self) -> *const T {
        self.ptr.cast_const()
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }
    #[inline]
    fn index(&self, index: usize) -> usize {
        wrap_index_pow2(index, self.len)
    }
    #[inline]
    fn multi_index(&self, channel_len: usize, channel: usize, index: usize) -> usize {
        let channel_start = channel * channel_len;
        self.index(channel_start + index)
    }
    #[inline]
    pub(crate) fn mutate_at_index(&self, index: usize) -> &mut T {
        let index = self.index(index);
        unsafe { &mut *self.mut_ptr().add(index) }
    }
    #[inline]
    pub(crate) fn read_at_index(&self, index: usize) -> T {
        let index = self.index(index);
        unsafe { self.const_ptr().add(index).read() }
    }
    #[inline]
    pub(crate) fn write_at_index(&self, index: usize, value: T) {
        let index = self.index(index);
        unsafe { self.mut_ptr().add(index).write(value) }
    }
    #[inline]
    pub(crate) fn multi_mutate_at_index(
        &self,
        channel_len: usize,
        channel: usize,
        index: usize,
    ) -> &mut T {
        let index = self.multi_index(channel_len, channel, index);
        self.mutate_at_index(index)
    }
    #[inline]
    pub(crate) fn multi_read_at_index(
        &self,
        channel_len: usize,
        channel: usize,
        index: usize,
    ) -> T {
        let index = self.multi_index(channel_len, channel, index);
        self.read_at_index(index)
    }
    #[inline]
    pub(crate) fn multi_write_at_index(
        &self,
        channel_len: usize,
        channel: usize,
        index: usize,
        value: T,
    ) {
        let index = self.multi_index(channel_len, channel, index);
        self.write_at_index(index, value);
    }
}
impl<T> Drop for HeapArray<T> {
    fn drop(&mut self) {
        for i in 0..self.len {
            unsafe {
                self.ptr.add(i).drop_in_place();
            }
        }
        unsafe { Vec::from_raw_parts(self.ptr, 0, self.len) };
    }
}
#[inline]
fn remainder_pow2(lhs: usize, rhs: usize) -> usize {
    (rhs - 1) & lhs
}
#[inline]
fn wrap_index_pow2(index: usize, capacity: usize) -> usize {
    remainder_pow2(index, capacity)
}
#[test]
fn remainder_works() {
    for pow2 in 1..31 {
        let capacity = 2usize.pow(pow2);
        for index in 0..capacity {
            let wrapped = remainder_pow2(index, capacity);
            assert!(wrapped < capacity)
        }
    }
}
