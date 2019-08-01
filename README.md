# HPC Pack REST BVT

## Prerequisites

* Python 3.6. Other 3.x versions may be also OK but not tested.
* [requests](https://pypi.org/project/requests/)

## Runtime Envrionment Variables

* `bvt_hostname`: The name of host that serves the REST API.
* `bvt_username`: The name of user on API server.
* `bvt_password`: The password of the user.
* `bvt_username2`: The name of another user who will be a "service as client" user. This variable is optionally, and when it's present, the `bvt_username`'s user must be of role Administrator or Job Administrator.

## Run It

```
python3 test.py
```
