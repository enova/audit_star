# Audit Star [![Build Status](https://travis-ci.org/enova/audit_star.svg?branch=master)](https://travis-ci.org/enova/audit_star) [![Documentation](https://img.shields.io/badge/godoc-reference-5272B4.svg?style=flat-square)](https://godoc.org/github.com/enova/audit_star)

The purpose of audit_star is to maintain a history of all of the changes that happen to a database, and to store the history of those changes within the database itself.  Additional documentation is provided by clicking the docs badge above.

## Basic usage

1. Install [Go](https://golang.org/)
2. ```go get github.com/enova/audit_star```
3. configure the audit.yml file for that database
4. ```./audit_star```

## Documentation

[General Description](docs/index.md)
[Deployment](docs/deployment.md)
[Testing](docs/testing.md)

## Contributing

Contribution to improve this project in any way are always welcome. Steps:

1. Fork project
2. Make changes in a separate branch
3. Write tests
4. Make a PR and mention the authors

## Contributors
* [Sri Rangarajan](https://github.com/Slania)
* [Sam Elston]
* [Kelmer Perez]
* [Youssef Kaiboussi](https://github.com/YoussefKaib)

## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
