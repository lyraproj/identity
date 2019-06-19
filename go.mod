module github.com/lyraproj/identity

require (
	github.com/hashicorp/go-hclog v0.8.0
	github.com/lyraproj/pcore v0.0.0-20190618142417-30605b6ee043
	github.com/lyraproj/semver v0.0.0-20181213164306-02ecea2cd6a2
	github.com/lyraproj/servicesdk v0.0.0-20190618142858-870593a059dc
	github.com/stretchr/testify v1.3.0
	go.etcd.io/bbolt v1.3.2
)

replace github.com/lyraproj/pcore => github.com/thallgren/pcore v0.0.0-20190619151240-bebc8c351bb4

replace github.com/lyraproj/servicesdk => github.com/thallgren/servicesdk v0.0.0-20190619152445-7481da553aae
