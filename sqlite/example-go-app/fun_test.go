package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoad(t *testing.T) {
	err := run()
	require.NoError(t, err)
}
