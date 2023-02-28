package s3db

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSortOrder(t *testing.T) {
	require.Equal(t, 1, NewKey(4).Order(NewKey(3.14)))
	require.Equal(t, -1, NewKey(3.14).Order(NewKey(4)))
	require.Equal(t, 1, NewKey("string").Order(NewKey(4)))
	require.Equal(t, -1, NewKey(4).Order(NewKey("string")))
	require.Equal(t, 1, NewKey([]byte("blob")).Order(NewKey("string")))
	require.Equal(t, -1, NewKey("string").Order(NewKey([]byte("blob"))))
}
