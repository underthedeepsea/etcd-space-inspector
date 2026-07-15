package bbolt

import "context"

// Summary is the file-level physical space composition.
type Summary struct {
	PhysicalFileSize   int64   `json:"physicalFileSize"`
	PageSize           int64   `json:"pageSize"`
	PageCount          int64   `json:"pageCount"`
	InUsePageBytes     int64   `json:"inUsePageBytes"`
	FreePageBytes      int64   `json:"freePageBytes"`
	FragmentationRatio float64 `json:"fragmentationRatio"`
	MetaPages          int64   `json:"metaPages"`
	BranchPages        int64   `json:"branchPages"`
	LeafPages          int64   `json:"leafPages"`
	FreelistPages      int64   `json:"freelistPages"`
	OverflowPages      int64   `json:"overflowPages"`
	FreePages          int64   `json:"freePages"`
	UnknownPages       int64   `json:"unknownPages"`
}

// PageStat describes one base page and its overflow span.
type PageStat struct {
	PageID      int64   `json:"pageId"`
	Type        string  `json:"pageType"`
	Overflow    int64   `json:"overflow"`
	TotalBytes  int64   `json:"totalBytes"`
	UsedBytes   int64   `json:"usedBytes"`
	FreeBytes   int64   `json:"freeBytes"`
	Utilization float64 `json:"utilization"`
}

// BucketStat contains public bbolt allocation statistics for one bucket path.
type BucketStat struct {
	Path          string `json:"bucketPath"`
	Depth         int64  `json:"depth"`
	KeyCount      int64  `json:"keyCount"`
	BranchBytes   int64  `json:"branchBytes"`
	LeafBytes     int64  `json:"leafBytes"`
	OverflowBytes int64  `json:"overflowBytes"`
	TotalBytes    int64  `json:"totalBytes"`
	UsedBytes     int64  `json:"usedBytes"`
}

// Sink receives bounded batches without retaining source pages.
type Sink interface {
	StorePages(context.Context, []PageStat) error
	StoreBuckets(context.Context, []BucketStat) error
}
