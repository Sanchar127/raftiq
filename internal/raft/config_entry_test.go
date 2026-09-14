package raft

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sanchar127/raftiq/internal/model"
)

func TestConfigurationEntryRoundTrip(t *testing.T) {
	configuration := model.Configuration{
		Voters: []model.NodeID{
			"node-1",
			"node-2",
			"node-3",
		},
	}

	data, err := EncodeConfigurationEntry(configuration)
	require.NoError(t, err)

	require.True(t, IsConfigurationEntry(data))

	decoded, err := DecodeConfigurationEntry(data)
	require.NoError(t, err)

	require.Equal(t, configuration, decoded)
}

func TestConfigurationEntrySingleVoter(t *testing.T) {
	configuration := model.Configuration{
		Voters: []model.NodeID{"node-1"},
	}

	data, err := EncodeConfigurationEntry(configuration)
	require.NoError(t, err)

	decoded, err := DecodeConfigurationEntry(data)
	require.NoError(t, err)

	require.Equal(t, configuration, decoded)
}

func TestEncodeConfigurationEntryRejectsEmptyConfiguration(t *testing.T) {
	_, err := EncodeConfigurationEntry(model.Configuration{})

	require.Error(t, err)
}

func TestEncodeConfigurationEntryRejectsDuplicateVoters(t *testing.T) {
	configuration := model.Configuration{
		Voters: []model.NodeID{
			"node-1",
			"node-2",
			"node-1",
		},
	}

	_, err := EncodeConfigurationEntry(configuration)

	require.Error(t, err)
}

func TestEncodeConfigurationEntryRejectsEmptyVoter(t *testing.T) {
	configuration := model.Configuration{
		Voters: []model.NodeID{
			"node-1",
			"",
		},
	}

	_, err := EncodeConfigurationEntry(configuration)

	require.Error(t, err)
}

func TestDecodeConfigurationEntryRejectsInvalidMagic(t *testing.T) {
	configuration := model.Configuration{
		Voters: []model.NodeID{"node-1"},
	}

	data, err := EncodeConfigurationEntry(configuration)
	require.NoError(t, err)

	binary.BigEndian.PutUint32(data[:4], 0xDEADBEEF)

	_, err = DecodeConfigurationEntry(data)

	require.Error(t, err)
}

func TestDecodeConfigurationEntryRejectsInvalidVersion(t *testing.T) {
	configuration := model.Configuration{
		Voters: []model.NodeID{"node-1"},
	}

	data, err := EncodeConfigurationEntry(configuration)
	require.NoError(t, err)

	data[4] = 99

	_, err = DecodeConfigurationEntry(data)

	require.Error(t, err)
}

func TestDecodeConfigurationEntryRejectsInvalidEntryType(t *testing.T) {
	configuration := model.Configuration{
		Voters: []model.NodeID{"node-1"},
	}

	data, err := EncodeConfigurationEntry(configuration)
	require.NoError(t, err)

	data[5] = 99

	_, err = DecodeConfigurationEntry(data)

	require.Error(t, err)
}

func TestDecodeConfigurationEntryRejectsTruncatedEntry(t *testing.T) {
	configuration := model.Configuration{
		Voters: []model.NodeID{
			"node-1",
			"node-2",
		},
	}

	data, err := EncodeConfigurationEntry(configuration)
	require.NoError(t, err)

	require.Greater(t, len(data), 1)

	_, err = DecodeConfigurationEntry(data[:len(data)-1])

	require.Error(t, err)
}

func TestDecodeConfigurationEntryRejectsTrailingData(t *testing.T) {
	configuration := model.Configuration{
		Voters: []model.NodeID{"node-1"},
	}

	data, err := EncodeConfigurationEntry(configuration)
	require.NoError(t, err)

	data = append(data, 0xFF)

	_, err = DecodeConfigurationEntry(data)

	require.Error(t, err)
}

func TestIsConfigurationEntry(t *testing.T) {
	configuration := model.Configuration{
		Voters: []model.NodeID{"node-1"},
	}

	data, err := EncodeConfigurationEntry(configuration)
	require.NoError(t, err)

	require.True(t, IsConfigurationEntry(data))
	require.False(t, IsConfigurationEntry(nil))
	require.False(t, IsConfigurationEntry([]byte{0x01, 0x02, 0x03}))
	require.False(t, IsConfigurationEntry([]byte("regular KV command")))
}
