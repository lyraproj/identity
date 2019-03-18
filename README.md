# Identity

This is an implementation of the Identity service that is based on the [Bolt](https://github.com/etcd-io/bbolt) key/value store. It implements the interface:

```
// Identity defines the API for services that track mappings between internal and external IDs
type Identity interface {
	BumpEra() error
	ReadEra() (era int64, err error)
	Associate(internalID string, externalID string) error
	GetExternal(internalID string) (string, error)
	GetInternal(externalID string) (string, error)
	PurgeExternal(externalID string) error
	PurgeInternal(internalID string) error
	RemoveExternal(externalID string) error
	RemoveInternal(internalID string) error
	Search(internalIDPrefix string) (px.List, error)
	Sweep(internalIDPrefix string) error
	Garbage() (px.List, error)
}
```
