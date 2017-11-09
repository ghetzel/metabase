package metabase

type GroupSet []Group

func (self GroupSet) Len() int {
	return len(self)
}

func (self GroupSet) Less(i, j int) bool {
	if mine, err := self[i].GetLatestModifyTime(); err == nil {
		if theirs, err := self[j].GetLatestModifyTime(); err == nil {
			return mine.Before(theirs)
		}
	}

	return false
}

func (self GroupSet) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}
