# Reposrt of performance

sqlite:
single operation write: 44 w/s
bulk operation write:  3.262 w/s

ristretto cache:
bulk operation write:  6.534 w/s

sqlite + bbolt database cache:
bulk write: 167 w/s