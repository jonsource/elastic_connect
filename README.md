# Development

## documentation

generate documentation using sphinx
```
sphinx-autobuild docs docs/_build/html
```


## testing

testing against one elastic search cluster
```
docker-compose up
pytest
```

testing against two elastic search clusters
```
docker-compose -f docker-compose-two.yml
pytest --namespace
```
