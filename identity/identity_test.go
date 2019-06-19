package identity

import (
	"github.com/lyraproj/pcore/pcore"
	"os"
	"testing"

	"github.com/lyraproj/pcore/px"
	"github.com/lyraproj/servicesdk/serviceapi"
	"github.com/stretchr/testify/require"
)

func deleteFile(filename string) {
	_, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		panic(err)
	}
	err = os.Remove(filename)
	if err != nil {
		panic(err)
	}
}

func checkGetInternal(t *testing.T, c px.Context, id serviceapi.Identity, externalID, internalID string) {
	actual, found := id.GetInternal(c, externalID)
	if internalID == "" {
		require.False(t, found)
	} else {
		require.True(t, found)
		require.Equal(t, internalID, actual)
	}
}

func checkGetExternal(t *testing.T, c px.Context, id serviceapi.Identity, internalID, externalID string) {
	actual, found := id.GetExternal(c, internalID)
	if externalID == "" {
		require.False(t, found)
	} else {
		require.True(t, found)
		require.Equal(t, externalID, actual)
	}
}

func TestBasicFunctionality(t *testing.T) {
	pcore.Do(func(c px.Context) {
		// Set up a clean DB
		filename := "TestBasicFunctionality.db"
		deleteFile(filename)
		defer deleteFile(filename)
		id := NewIdentity(filename)

		// Check there is no mapping
		checkGetExternal(t, c, id, "i1", "")
		checkGetInternal(t, c, id, "e1", "")
		checkGetExternal(t, c, id, "foo", "")
		checkGetInternal(t, c, id, "bar", "")

		// Insert something
		id.Associate(c, "i1", "e1")

		// Check there is now a mapping
		checkGetExternal(t, c, id, "i1", "e1")
		checkGetInternal(t, c, id, "e1", "i1")
		checkGetExternal(t, c, id, "foo", "")
		checkGetInternal(t, c, id, "bar", "")
	})
}

func TestBasicFunctionalityAcrossInstances(t *testing.T) {
	pcore.Do(func(c px.Context) {
		// Set up a clean DB
		filename := "TestBasicFunctionalityAcrossInstances.db"
		deleteFile(filename)
		defer deleteFile(filename)
		id := NewIdentity(filename)

		// Insert something
		id.Associate(c, "i1", "e1")

		//new up another identity service
		id = NewIdentity(filename)

		// Check there is now a mapping
		checkGetExternal(t, c, id, "i1", "e1")
		checkGetInternal(t, c, id, "e1", "i1")
	})
}

func TestMultipleKeys(t *testing.T) {
	pcore.Do(func(c px.Context) {
		// Set up a clean DB
		filename := "TestMultipleKeys.db"
		deleteFile(filename)
		defer deleteFile(filename)
		id := NewIdentity(filename)

		// Check there is no mapping
		checkGetExternal(t, c, id, "i1", "")
		checkGetExternal(t, c, id, "i2", "")
		checkGetExternal(t, c, id, "i3", "")
		checkGetExternal(t, c, id, "i4", "")
		checkGetInternal(t, c, id, "e1", "")
		checkGetInternal(t, c, id, "e2", "")
		checkGetInternal(t, c, id, "e3", "")
		checkGetInternal(t, c, id, "e4", "")

		// Insert something
		id.Associate(c, "i1", "e1")
		id.Associate(c, "i2", "e2")
		id.Associate(c, "i3", "e3")
		id.Associate(c, "i4", "e4")

		// Check there is now a mapping
		checkGetExternal(t, c, id, "i1", "e1")
		checkGetExternal(t, c, id, "i2", "e2")
		checkGetExternal(t, c, id, "i3", "e3")
		checkGetExternal(t, c, id, "i4", "e4")
		checkGetInternal(t, c, id, "e1", "i1")
		checkGetInternal(t, c, id, "e2", "i2")
		checkGetInternal(t, c, id, "e3", "i3")
		checkGetInternal(t, c, id, "e4", "i4")

		// Change something
		id.Associate(c, "i1", "e4")
		id.Associate(c, "i2", "e3")
		id.Associate(c, "i3", "e2")

		// Check the mappings update correctly
		checkGetExternal(t, c, id, "i1", "e4")
		checkGetExternal(t, c, id, "i2", "e3")
		checkGetExternal(t, c, id, "i3", "e2")
		checkGetExternal(t, c, id, "i4", "")
		checkGetInternal(t, c, id, "e1", "")
		checkGetInternal(t, c, id, "e2", "i3")
		checkGetInternal(t, c, id, "e3", "i2")
		checkGetInternal(t, c, id, "e4", "i1")
	})

}

func TestRemove(t *testing.T) {
	pcore.Do(func(c px.Context) {
		// Set up a clean DB
		filename := "TestMultipleKeys.db"
		deleteFile(filename)
		defer deleteFile(filename)
		id := NewIdentity(filename)

		// Insert something
		id.Associate(c, "i1", "e1")
		id.Associate(c, "i2", "e2")
		id.Associate(c, "i3", "e3")
		id.Associate(c, "i4", "e4")

		// Check there is now a mapping
		checkGetExternal(t, c, id, "i1", "e1")
		checkGetExternal(t, c, id, "i2", "e2")
		checkGetExternal(t, c, id, "i3", "e3")
		checkGetInternal(t, c, id, "e1", "i1")
		checkGetInternal(t, c, id, "e2", "i2")
		checkGetInternal(t, c, id, "e3", "i3")

		// Remove valid keys
		id.RemoveExternal(c, "e3")
		id.RemoveInternal(c, "i2")

		// Remove invalid keys
		id.RemoveInternal(c, "e1") // External ID used where internal expected
		id.RemoveInternal(c, "foo")

		// Check the mappings update correctly
		checkGetExternal(t, c, id, "i1", "e1")
		checkGetExternal(t, c, id, "i2", "")
		checkGetExternal(t, c, id, "i3", "")
		checkGetInternal(t, c, id, "e1", "i1")
		checkGetInternal(t, c, id, "e2", "")
		checkGetInternal(t, c, id, "e3", "")
	})

}

func TestErrors(t *testing.T) {
	pcore.Do(func(c px.Context) {
		// Set up a clean DB
		filename := "TestErrors.db"
		deleteFile(filename)
		defer deleteFile(filename)
		id := NewIdentity(filename)

		// Insert something invalid
		require.Panics(t, func() { id.Associate(c, "i1", "") })
		require.Panics(t, func() { id.Associate(c, "", "e1") })
	})
}

func TestSearch(t *testing.T) {
	pcore.Do(func(c px.Context) {
		// Set up a clean DB
		filename := "TestSearch.db"
		deleteFile(filename)
		defer deleteFile(filename)
		id := NewIdentity(filename)

		// Insert something
		id.Associate(c, "a:i1", "e1")
		id.Associate(c, "a:i2", "e2")
		id.Associate(c, "b:i3", "e3")
		id.Associate(c, "b:i4", "e4")

		mappings := id.Search(c, "a:")
		require.EqualValues(t, 2, mappings.Len())
	})
}

func TestBumpEra(t *testing.T) {
	pcore.Do(func(c px.Context) {
		filename := "TestBumpEra.db"
		deleteFile(filename)
		defer deleteFile(filename)
		id := NewIdentity(filename)
		id.BumpEra(c)
		era := id.(*identity).ReadEra(c)
		require.EqualValues(t, int64(1), era)
	})
}

func TestAccessSetEra(t *testing.T) {
	pcore.Do(func(c px.Context) {
		filename := "TestAccessSetEra.db"
		deleteFile(filename)
		defer deleteFile(filename)
		id := NewIdentity(filename)

		// Insert something
		id.Associate(c, "a:i1", "e1")
		id.Associate(c, "a:i2", "e2")

		// Check that era is zero
		mappings := id.Search(c, "a:")
		require.EqualValues(t, 2, mappings.Len())
		require.EqualValues(t, int64(0), mappings.At(0).(px.List).At(3).(px.Number).Int())
		require.EqualValues(t, int64(0), mappings.At(1).(px.List).At(3).(px.Number).Int())

		// Bump era
		id.BumpEra(c)

		// Access using getExternal
		checkGetExternal(t, c, id, "a:i1", "e1")

		// Check that era is one on the accessed element and zero
		// on the one that wasn't accessed
		mappings = id.Search(c, "a:")
		require.EqualValues(t, 2, mappings.Len())
		require.EqualValues(t, int64(1), mappings.At(0).(px.List).At(3).(px.Number).Int())
		require.EqualValues(t, int64(0), mappings.At(1).(px.List).At(3).(px.Number).Int())
	})
}

func TestSweep(t *testing.T) {
	pcore.Do(func(c px.Context) {
		filename := "TestSearchGarbage.db"
		deleteFile(filename)
		defer deleteFile(filename)
		id := NewIdentity(filename)

		// Insert something
		id.Associate(c, "a:i1", "e1")
		id.Associate(c, "a:i2", "e2")
		id.Associate(c, "a:i3", "e3")

		// Bump era
		id.BumpEra(c)

		// Access using getExternal
		checkGetExternal(t, c, id, "a:i1", "e1")
		checkGetExternal(t, c, id, "a:i2", "e2")
		id.RemoveInternal(c, "a:i2")

		// Check that element that wasn't accessed is found by SearchGarbage
		id.Sweep(c, "a:")

		// Retrieve the garbage bin
		garbage := id.Garbage(c, "")
		require.EqualValues(t, 2, garbage.Len())

		// Accessed between BumpEra and Sweep and then explicitly removed
		require.EqualValues(t, "e2", garbage.At(0).(px.List).At(1).String())
		require.EqualValues(t, int64(1), garbage.At(0).(px.List).At(3).(px.Number).Int())

		// Never accessed between BumpEra and Sweep
		require.EqualValues(t, "e3", garbage.At(1).(px.List).At(1).String())
		require.EqualValues(t, int64(0), garbage.At(1).(px.List).At(3).(px.Number).Int())
	})
}

func TestSweepWithRef(t *testing.T) {
	pcore.Do(func(c px.Context) {
		filename := "TestSearchGarbage.db"
		deleteFile(filename)
		defer deleteFile(filename)
		id := NewIdentity(filename)

		// Insert something
		id.Associate(c, "a:i1", "e1")
		id.Associate(c, "a:i2", "e2")
		id.AddReference(c, "a:i3", "b:")
		id.Associate(c, "b:i1", "e3")
		id.Associate(c, "b:i2", "e4")

		// Bump era
		id.BumpEra(c)

		// Access using getExternal
		checkGetExternal(t, c, id, "a:i1", "e1")
		checkGetExternal(t, c, id, "b:i1", "e3")

		// Check that element that wasn't accessed is found by SearchGarbage
		id.Sweep(c, "a:")

		// Retrieve the garbage bin
		garbage := id.Garbage(c, "a:")
		require.EqualValues(t, 2, garbage.Len())

		// Never accessed between BumpEra and Sweep
		require.EqualValues(t, "a:i2", garbage.At(0).(px.List).At(0).String())
		require.EqualValues(t, "e2", garbage.At(0).(px.List).At(1).String())
		require.EqualValues(t, "b:i2", garbage.At(1).(px.List).At(0).String())
		require.EqualValues(t, "e4", garbage.At(1).(px.List).At(1).String())
	})
}

func TestPurge(t *testing.T) {
	pcore.Do(func(c px.Context) {
		filename := "TestPurge.db"
		deleteFile(filename)
		defer deleteFile(filename)
		id := NewIdentity(filename)

		// Insert something
		id.Associate(c, "a:i1", "e1")
		id.Associate(c, "a:i2", "e2")
		id.Associate(c, "a:i3", "e3")

		// Bump era
		id.BumpEra(c)

		// Access using getExternal
		checkGetExternal(t, c, id, "a:i1", "e1")
		checkGetExternal(t, c, id, "a:i2", "e2")
		id.RemoveInternal(c, "a:i2")

		// Check that element that wasn't accessed is found by SearchGarbage
		id.Sweep(c, "a:")

		// Purge
		id.PurgeExternal(c, "e1")
		id.PurgeInternal(c, "a:i2")

		checkGetExternal(t, c, id, "a:i1", "")
		checkGetExternal(t, c, id, "a:i2", "")

		// Retrieve the garbage bin
		garbage := id.Garbage(c, "")
		require.EqualValues(t, 1, garbage.Len())

		// Not purged
		require.EqualValues(t, "e3", garbage.At(0).(px.List).At(1).String())
	})
}
