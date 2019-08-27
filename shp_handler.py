import os
import zipfile

from datetime import datetime
from osgeo import ogr, osr


os.environ['SHAPE_ENCODING'] = "cp1251"


class ShpHandler(object):
    """
    Handler for work with shp files.
    """

    driver = ogr.GetDriverByName('ESRI Shapefile')

    def __init__(self, shp_file_path):
        self._shp_file_path = shp_file_path
        self._shp_file = ogr.Open(self._shp_file_path)
        self._layer = self._shp_file.GetLayer()
        self._ldfn = self._layer.GetLayerDefn()
        self._field_names = self._get_field_names()
        self._file_name = os.path.basename(self._shp_file_path).rsplit('.', 1)[0]
        self._path = os.path.dirname(os.path.abspath(self._shp_file_path))
        self._proj = self._layer.GetSpatialRef()

    def copy_ds(self, path_to_new_ds, layer_name='result', ):
        """
        Create copy for self shape file
        :param path_to_new_ds: path to new file
        :param layer_name: name for layer
        :return: data source
        """
        ds = self.driver.CreateDataSource(path_to_new_ds)
        mem = ds.CopyLayer(self._layer, layer_name, ['OVERWRITE=YES'])
        ds = None
        ds = ogr.Open(path_to_new_ds, update=1)
        return ds

    def _get_field_names(self):
        """
        Get field names (dbf column names)
        :return: set of field names
        """
        shp_field_names = set()
        for i in range(self._ldfn.GetFieldCount()):
            fdefn = self._ldfn.GetFieldDefn(i)
            shp_field_names.add(fdefn.name)
        return shp_field_names

    def features_to_list_of_dict(self, wkt_field_name, transform=None):
        """
        Get list of feature dicts
        :param wkt_field_name: name for wkt key of dict
        :param transform: projection transform
        :return: list of feature dicts with wkt key
        """
        features = []
        for forjoin_feature in self._layer:
            forjoin = {field: forjoin_feature.GetField(field) for field in self._field_names}
            forjoin_geom = forjoin_feature.GetGeometryRef()
            if forjoin_geom:
                if transform is not None:
                    forjoin_geom.Transform(transform)
                forjoin.update({wkt_field_name: ogr.CreateGeometryFromWkt(forjoin_geom.ExportToWkt())})
                features.append(forjoin)
        return features

    def get_transform(self, proj_to):
        """
        Get transform projection
        :param proj_to: projection for transform
        :return: transform
        """
        return osr.CoordinateTransformation(self._proj, proj_to)

    def _set_for_join_fields(self, other, dest_layer, fields=None):
        """
        :param other: joined ShpHandler (ShpHandler)
        :param dest_layer: joining layer (layer)
        :param fields: fields for join (list)
        :return: dict of joined fields (dict)
        """
        field_names = self._field_names
        if fields is None:
            forjoinfields_dict = {f: f for f in other._field_names}
        else:
            forjoinfields_dict = {f: f for f in fields}
        for field, join_field in forjoinfields_dict.items():
            new_name = join_field
            name_num = 0
            while new_name in field_names:
                name_num += 1
                new_name = f'new{name_num}'
            newfield = ogr.FieldDefn(new_name, ogr.OFTString)
            dest_layer.CreateField(newfield)
            forjoinfields_dict[field] = new_name
            field_names.add(new_name)
        return forjoinfields_dict

    def get_default_join_name(self, other):
        """
        Get name for shape file: joiningname_join_joinedname__YYYYmmdd_HHMMSS
        :param other: joined ShpHandler (ShpHandler)
        :return: str
        """
        datestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        name = '_join_'.join([self._file_name, other._file_name])
        name += '__' + datestamp
        return name

    def spatial_join(self, other, sep, path_to_new_ds=None, joined_fields=None, wkt_field_name='geometry_wkt'):
        """
        :param other: joined ShpHandler (ShpHandler)
        :param sep: separator(str)
        :param path_to_new_ds: str if None get from get_default_join_name
        :param joined_fields: list
        :param wkt_field_name: str
        :return: None
        """
        if path_to_new_ds is None:
            path_to_new_ds = self.get_default_join_name(other)
        _path_to_new_ds = path_to_new_ds + '.shp'

        if not _path_to_new_ds.endswith('.shp'):
            _path_to_new_ds += '.shp'

        ds_copy = self.copy_ds(path_to_new_ds=_path_to_new_ds)
        layer_copy = ds_copy.GetLayer()
        try:
            feature_count = layer_copy.GetFeatureCount()
            forjoinfields_dict = self._set_for_join_fields(other, layer_copy, joined_fields)
            transform = other.get_transform(self._proj)
            forjoin_features = other.features_to_list_of_dict(wkt_field_name=wkt_field_name, transform=transform)

            n = 0
            for tojoin_feature in layer_copy:
                n += 1
                tojoin_geom = tojoin_feature.GetGeometryRef()

                if tojoin_geom:
                    for forjoin in forjoin_features:
                        if tojoin_geom.Within(forjoin[wkt_field_name]) or tojoin_geom.Intersect(forjoin[wkt_field_name]) or \
                                forjoin[wkt_field_name].Within(tojoin_geom):
                            for field, new_field in forjoinfields_dict.items():
                                tojoin_val = tojoin_feature.GetField(new_field)
                                forjoin_val = forjoin[field]
                                if forjoin_val:
                                    forjoin_val = str(forjoin_val)
                                    if tojoin_val and (forjoin_val in tojoin_val):
                                        continue
                                vals = [str(v) for v in (tojoin_val, forjoin_val) if v]
                                vals.sort(key=lambda s: s.lower())
                                new_val = f'{sep} '.join(vals)
                                tojoin_feature.SetField(new_field, new_val.strip())

                    layer_copy.SetFeature(tojoin_feature)
                progress = int((feature_count - n) / feature_count * 100)
                if progress % 10 == 0:
                    print(f'progress: [{"#" * (progress//10)}{" " * (10-progress//10)}] {n}/{feature_count} - {progress}', end='\r')
            ds_copy = None
        except Exception as e:
            if os.path.exists(_path_to_new_ds):
                self.driver.DeleteDataSource(_path_to_new_ds)
            raise e

    @classmethod
    def to_zip(cls, path_to_shp, delete_files=False):
        """
        Zipping shapefile
        :param path_to_shp: str
        :param delete_files: if True, source will deleted
        :return: None
        """
        zipfile_path = path_to_shp.rsplit('.', 1)[0] + '.zip'
        try:
            z = zipfile.ZipFile(zipfile_path, 'w')
            file_name = os.path.basename(path_to_shp).rsplit('.', 1)[0]
            path_to_geofiles = os.path.dirname(os.path.abspath(path_to_shp))

            for geo_file in os.listdir(path_to_geofiles):
                if file_name == geo_file.rsplit('.', 1)[0] and geo_file[-4:] != '.zip':
                    geo_file_path = os.path.join(path_to_geofiles, geo_file)
                    z.write(geo_file_path, geo_file)
            z.close()
        except Exception as e:
            if os.path.exists(zipfile_path):
                os.remove(zipfile_path)
            raise e
        finally:
            if os.path.exists(path_to_shp) and delete_files:
                cls.driver.DeleteDataSource(path_to_shp)
