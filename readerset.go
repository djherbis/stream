package stream

type readerSet map[*Reader]struct{}

func newReaderSet() *readerSet {
	rs := make(readerSet)
	return &rs
}

func (rs *readerSet) add(r *Reader) *Reader {
	(*rs)[r] = struct{}{}
	return r
}

func (rs *readerSet) has(r *Reader) bool {
	_, ok := (*rs)[r]
	return ok
}

func (rs *readerSet) drop(r *Reader) *Reader {
	delete(*rs, r)
	return r
}

func (rs *readerSet) dropAll() (dropped []*Reader) {
	dropped = make([]*Reader, len(*rs), len(*rs))
	i := 0
	for r := range *rs {
		dropped[i] = r
		i++
	}
	*rs = make(readerSet)
	return dropped
}
