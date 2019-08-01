# HPC Pack REST BVT

## Prerequisites

* Python 3.6. Other 3.x versions may be also OK but not tested.
* [requests](https://pypi.org/project/requests/)

## Runtime Envrionment 

The following envrionment variables must be set:

* `bvt_hostname`: The name of host that serves the REST API.
* `bvt_username`: The name of user on API server.
* `bvt_password`: The password of the user.

The following envrionment variables are optional:
* `bvt_username2`: The name of another user who will be a "service as client" user. This variable is optionally, and when it's present, the `bvt_username`'s user must be of role Administrator or Job Administrator. If this envrionment variable is absent, "service as client" test will be skipped.

`bvt_username` must have its credential saved on server already, and so does `bvt_username2` if it's present.

## Run It

```
python3 test.py
```
