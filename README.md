# shp_handler
### Shapefiles handler. Wrapper for python gdal.

### tools:
- spatial join
- zipping

### how to use:

```python
from shp_tools import ShpHandler

to_join = ShpHandler(path)
for_join = ShpHandler(path)
to_join.spatial_join(for_join, sep=',')

```
