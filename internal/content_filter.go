package internal

import (
	"fmt"
	"strings"

	"github.com/ttab/newsdoc"
)

func NewContentFilterFromParams(p Parameters) (*ContentFilter, error) {
	cf := ContentFilter{
		types: make(map[string][]BlockFilter),
	}

	for _, exp := range p.IgnoreSections {
		docType, sectionUUID, ok := strings.Cut(exp, ":")
		if !ok {
			return nil, fmt.Errorf("invalid section filter %q", exp)
		}

		matcher := newsdoc.BlockMatchFunc(func(block newsdoc.Block) bool {
			return block.Rel == "section" && block.UUID == sectionUUID
		})

		cf.types[docType] = append(cf.types[docType],
			BlockFilter{
				Kind:    BlockKindLink,
				Matcher: matcher,
			})
	}

	return &cf, nil
}

type ContentFilter struct {
	types map[string][]BlockFilter
}

type BlockKind string

const (
	BlockKindLink    BlockKind = "link"
	BlockKindMeta    BlockKind = "meta"
	BlockKindContent BlockKind = "content"
)

type BlockFilter struct {
	Kind    BlockKind
	Matcher newsdoc.BlockMatcher
}

func (cf *ContentFilter) HasFilters(docType string) bool {
	return len(cf.types[docType]) > 0
}

// Checks if a document passes the filters and returns true if it does.
func (cf *ContentFilter) Check(doc newsdoc.Document) bool {
	for _, f := range cf.types[doc.Type] {
		var list []newsdoc.Block

		switch f.Kind {
		case BlockKindLink:
			list = doc.Links
		case BlockKindMeta:
			list = doc.Meta
		case BlockKindContent:
			list = doc.Content
		}

		_, ok := newsdoc.FirstBlock(list, f.Matcher)
		if ok {
			return false
		}
	}

	return true
}
