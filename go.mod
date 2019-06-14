module github.com/lyraproj/identity

require (
	github.com/hashicorp/go-hclog v0.8.0
	github.com/lyraproj/pcore v0.0.0-20190606102217-7824aee25201
	github.com/lyraproj/semver v0.0.0-20181213164306-02ecea2cd6a2
	github.com/lyraproj/servicesdk v0.0.0-20190607070716-322c167d24a9
	github.com/stretchr/testify v1.3.0
	go.etcd.io/bbolt v1.3.2
)

replace github.com/lyraproj/servicesdk => github.com/thallgren/servicesdk v0.0.0-20190614075654-f02c272830a4
