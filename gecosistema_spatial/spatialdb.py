#-------------------------------------------------------------------------------
# Licence:
# Copyright (c) 2012-2019 Valerio for Gecosistema S.r.l.
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
#
# Name:        module.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:
#-------------------------------------------------------------------------------
from gecosistema_core import *
from gecosistema_database import *

import unicodecsv as csv
import sqlite3
import ogr,osr

from .getvalueat import GetValueAt

class SpatialDB(SqliteDB):

    def __init__(self, filename, modules=[]):
        """
        Constructor
        :param filename:
        """
        SqliteDB.__init__(self, filename, ["mod_spatialite"] + modules)
        #self.CreateSpatialReferenceTable()
        #self.CreateGeometryColumnTable()
        self.conn.create_function("GetValueAt", 3, GetValueAt)
        if not self.TableExists("spatial_ref_sys"):
            self.execute("""SELECT InitSpatialMetaData();""")

    def CreateSpatialReferenceTable(self):
        sql = """
        CREATE TABLE IF NOT EXISTS [spatial_ref_sys] (
          [srid] INTEGER NOT NULL PRIMARY KEY,
          [auth_name] TEXT NOT NULL,
          [auth_srid] INTEGER NOT NULL,
          [ref_sys_name] TEXT NOT NULL DEFAULT 'Unknown',
          [proj4text] TEXT NOT NULL,
          [srtext] TEXT NOT NULL DEFAULT 'Undefined');
        --INSERT OR REPLACE INTO [spatial_ref_sys](srid,auth_name,auth_srid,ref_sys_name,proj4text,srtext)
        --VALUES ({epsg},'epsg',{epsg},'epsg:'||{epsg},'{proj4text}','{srtext}');
        """
        self.execute(sql)

    def CreateGeometryColumnTable(self):
        sql = """
        CREATE TABLE IF NOT EXISTS [geometry_columns] (
          [f_table_name] VARCHAR,
          [f_geometry_column] VARCHAR,
          [geometry_type] INTEGER,
          [coord_dimension] INTEGER,
          [srid] INTEGER,
          [geometry_format] VARCHAR,
          PRIMARY KEY ([f_table_name]));
        """
        self.execute(sql)

    def GridFromExtent(self, layername, extent, dx=500.0, dy=None, verbose=False):
        """
        GridFromExtent -  Create a Receptor Grid
        """
        [minx, miny, maxx, maxy] = extent
        minx, miny, maxx, maxy = val(minx), val(miny), val(maxx), val(maxy)
        minx, miny, maxx, maxy = min(minx, maxx), min(miny, maxy), max(minx, maxx), max(miny, maxy)

        dx = float(dx)
        dy = float(dy) if dy else dx

        width = maxx - minx
        height = maxy - miny
        m, n = int(round(height / dy)), int(round(width / dx))

        rx = width - ((n - 1) * dx)
        ry = height - ((m - 1) * dy)

        values = []
        for i in range(m):
            for j in range(n):
                x = minx + (rx / 2.0) + (dx * j)
                y = miny + (ry / 2.0) + (dy * i)
##                point = ogr.Geometry(ogr.wkbPoint)
##                point.AddPoint_2D(x, y)
##                blob = sqlite3.Binary(point.ExportToWkb())
##                values.append((blob,))
                values.append((x,y,))

        env=  {"layername": layername}
        self.executeMany("""INSERT OR REPLACE INTO [{layername}](X,Y) VALUES(?,?);""", env, values, verbose=verbose)
        self.execute("""UPDATE [{layername}] SET [Geometry] = MakePoint([x],[y],3857);""",env)



    def createTableFromCSV(self, filename,
                           tablename="",
                           fieldx="x",
                           fieldy="y",
                           append=False,
                           Temp=False,
                           nodata=["", "Na", "NaN", "-", "--", "N/A"],
                           verbose=False):
        """
        createTableFromCSV - make a read-pass to detect data fieldtype
        """
        # ---------------------------------------------------------------------------
        #   Open the stream
        # ---------------------------------------------------------------------------
        with open(filename, "rb") as stream:

            # detect the dialect
            dialect = self.detectDialect(filename)
            sep = dialect.delimiter
            # ---------------------------------------------------------------------------
            #   decode data lines
            # ---------------------------------------------------------------------------
            fieldnames = []
            fieldtypes = []
            n = 1
            line_no = 0
            header_line_no = 0
            stream = self.skip_commented_or_empty_lines(stream)
            reader = csv.reader(stream, dialect, encoding="utf-8-sig")

            for line in reader:
                #line = [unicode(cell, 'utf-8-sig') for cell in line]
                if len(line) < n:
                    # skip empty lines
                    pass
                elif not fieldtypes:
                    n = len(line)
                    fieldtypes = [''] * n
                    fieldnames = line
                    header_line_no = line_no
                else:
                    fieldtypes = [SQLTYPES[min(SQLTYPES[item1], SQLTYPES[item2])] for (item1, item2) in
                                  zip(sqltype(line, nodata=nodata), fieldtypes)]

                line_no += 1

            self.createTable(tablename, ["ogc_fid"]+fieldnames, ["INTEGER PRIMARY KEY AUTOINCREMENT"]+fieldtypes, primarykeys = "", Temp=Temp, overwrite=not append,
                             verbose=verbose)
            return (fieldnames, fieldtypes, header_line_no, dialect)


    def importGeomsFromCsv(self, tablename, filename, fieldx="x",fieldy ="y",
                            append=False,
                            Temp=False,
                            nodata=["", "Na", "NaN", "-", "--", "N/A"],
                            verbose=False):
        """
        importGeomsFromCsv
        """
        if self.createTableFromCSV:
            (fieldnames, fieldtypes, header_line_no, dialect) = self.createTableFromCSV(filename, tablename, fieldx,fieldy,
                                                                               append, Temp, nodata, verbose)
        else:
            (fieldnames, fieldtypes, header_line_no, dialect) = [],[],0, self.detectDialect(filename)

        #search field idx
        fieldx,fieldy = lower(fieldx),lower(fieldy)
        if fieldx in lower(fieldnames) and fieldy in lower(fieldnames):
            idxx = lower(fieldnames).index(fieldx)
            idxy = lower(fieldnames).index(fieldy)
        else:
            idxx,idxy=-1,-1

        data = []
        line_no = 0
        with open(filename, "rb") as stream:
            self.skip_commented_or_empty_lines(stream)
            reader = csv.reader(stream, dialect)

            for line in reader:
                if line_no > header_line_no:
                    line = [unicode(cell, 'utf-8-sig') for cell in line]
                    if len(line) == len(fieldnames):
##                        x  = float(line[idxx]) if idxx>=0 and idxy>=0 else 0.0
##                        y  = float(line[idxy]) if idxx>=0 and idxy>=0 else 0.0
##                        point = ogr.Geometry(ogr.wkbPoint)
##                        point.AddPoint_2D(x, y)
##                        blob = sqlite3.Binary(point.ExportToWkb())
##                        geom =point.ExportToWkb()
##
##                        geom = "GeomFromText('POINT(%s %s)'"
                        data.append(line)

                line_no += 1

            values = [ parseValue(row,nodata) for row in data]

            env = {
                "tablename":tablename,
                "fieldnames": ",".join(wrap(fieldnames,"[","]")),
                "values": ",".join(["?"]*(len(fieldnames)))
            }
            self.executeMany("""INSERT OR REPLACE INTO [{tablename}]({fieldnames}) VALUES({values})""",env,values);

            env ={"layername":tablename,"geom_type":1,"epsg":"3857"}

            sql = """
            SELECT AddGeometryColumn('{layername}','Geometry',3857,'POINT',2);
            SELECT CreateSpatialIndex('{layername}','Geometry');
            UPDATE [{layername}] SET [Geometry] = MakePoint([x],[y],3857);"""
            self.execute(sql,env,verbose=True)

    def CreateShape(self, layername, fileshp="", fieldnames=""):
        """
        CreateShape
        """
        env = {"layername": layername}
        cur = self.execute(
            "SELECT f_geometry_column,geometry_type,srid FROM [geometry_columns] WHERE f_table_name='{layername}';",
            env, outputmode="array", verbose=True)
        if cur:
            f_geometry_column, geometry_type, srid = cur[0]
            print  f_geometry_column, geometry_type, srid
        else:
            return
        env["f_geometry_column"] = f_geometry_column
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(srid)

        fileshp = fileshp if fileshp else forceext(juststem(layername), "shp")
        mkdirs(justpath(fileshp))
        driver = ogr.GetDriverByName("ESRI Shapefile")
        if file(fileshp):
            driver.DeleteDataSource(fileshp)
        ds = driver.CreateDataSource(fileshp)
        layer = ds.CreateLayer(str(layername), srs, geometry_type)

        if fieldnames == "*":
            items = self.GetFieldNames(layername, "INTEGER|FLOAT|TEXT", typeinfo=True)
            fieldnames = [fieldname for fieldname, _ in items]
            for fieldname, ftype in items:
                if ftype == "INTEGER":
                    ogrtype = ogr.OFTInteger
                elif ftype == "FLOAT":
                    ogrtype = ogr.OFTReal
                elif ftype == "TEXT":
                    ogrtype = ogr.OFTString
                else:
                    ogrtype = ogr.OFTReal
                layer.CreateField(ogr.FieldDefn(str(fieldname)[:10], ogrtype))
        else:
            fieldnames = listify(fieldnames)

            for fieldname in fieldnames:
                layer.CreateField(ogr.FieldDefn(str(fieldname)[:10], ogr.OFTReal))

        features = self.execute("SELECT * FROM [{layername}];", env, outputmode="object", verbose=False)
        for row in features:
            #blob =  sqlite3.Binary(row[f_geometry_column])
            #geom = ogr.CreateGeometryFromWkb(str(blob) )
            geom = ogr.CreateGeometryFromWkb(str(row[f_geometry_column]))
            feature = ogr.Feature(layer.GetLayerDefn())
            feature.SetFID(row["ogc_fid"])
            for fieldname in fieldnames:
                if row.has_key(fieldname):
                    feature.SetField(str(fieldname)[:10], row[fieldname])
            feature.SetGeometry(geom)
            layer.CreateFeature(feature)
            feature = None

        return fileshp

    def ogrType(self,value):
        """
        infert type from value
        """
        if isdate(value):
            return ogr.OFTDate
        elif isdatetime(value):
            return ogr.OFTDateTime
        elif isinstance(value,(int,bool)):
            return ogr.OFTInteger
        elif isfloat(value):
            return ogr.OFTReal
        elif isstring(value):
            return ogr.OFTString
        elif isinstance(value, (buffer,)):
            return ogr.OFTBinary
        else:
            return ogr.OFTReal

    def ImportSpatialReferenceFrom(self, code):
        """
        ImportSpatialReferenceFrom
        """
        spatialRef = osr.SpatialReference()
        if isint(code):
            spatialRef.ImportFromEPSG(code)
        elif isstring(code) and code.startswith("epsg:"):
            code= code.replace("epsg:","")
            spatialRef.ImportFromEPSG(code)
        elif isstring(code) and isfile(code):
            wkt = filetostr(code)
            spatialRef.ImportFromWkt(wkt)
        else:
            spatialRef.ImportFromWkt(3857)

        return spatialRef

    def toShp(self, sql, env, fileshp, epsg=3857):
        """
        CreateShape

        SELECT
            AsBinary(MakePoint(X,Y)) as geometry,
            piezo as value
           FROM table;
        """
        layername  = str(juststem(fileshp))
        f_geometry_column = "geometry"
        srs = self.ImportSpatialReferenceFrom(epsg)

        mkdirs(justpath(fileshp))
        driver = ogr.GetDriverByName("ESRI Shapefile")
        if file(fileshp):
            driver.DeleteDataSource(fileshp)
        ds = driver.CreateDataSource(fileshp)

        #detect geometry type
        geometry_type =1 #default is POINT
        fieldnames,fieldtypes = [],[]
        sql_limit_1 = sql if "LIMIT" in upper(sql) else sql.strip("\r\n\t ;")+" LIMIT 1;"
        features = self.execute(sql_limit_1, env, outputmode="object", verbose=False)

        for feature in features:
            fieldnames = feature.keys()
            fieldtypes = [ self.ogrType(feature[key]) for key in fieldnames]

        #detect f_geometry_column
        for fieldname, fieldtype in zip(fieldnames, fieldtypes):
            if fieldtype == ogr.OFTBinary:
                f_geometry_column = fieldname
                geom = ogr.CreateGeometryFromWkb(str(feature[f_geometry_column]))
                geometry_type = geom.GetGeometryType()
                break

        #create the layer
        layer = ds.CreateLayer(str(layername), srs, geometry_type)

        # Add extra fields
        for fieldname,fieldtype in zip(fieldnames,fieldtypes):
            if fieldname != f_geometry_column:
                layer.CreateField(ogr.FieldDefn(str(fieldname)[:10], fieldtype))

        ogc_fid = 0
        features = self.execute(sql, env, outputmode="object", verbose=False)
        for row in features:
            #blob =  sqlite3.Binary(row[f_geometry_column])
            #geom = ogr.CreateGeometryFromWkb(str(blob) )
            geom = ogr.CreateGeometryFromWkb(str(row[f_geometry_column]))

            feature = ogr.Feature(layer.GetLayerDefn())
            feature.SetFID(ogc_fid)
            feature.SetGeometry(geom)
            for fieldname in fieldnames:
                if fieldname != f_geometry_column:
                    feature.SetField(str(fieldname)[:10], row[fieldname])

            layer.CreateFeature(feature)
            feature = None
            ogc_fid +=1

        return fileshp


    def RotatePlume(self,tablename):
        """
        RotatePlume
        """
        env = {"tablename":tablename,"temptable":tempname("temp-")}
        sql = """DROP TABLE IF  EXISTS [angles];CREATE TEMP TABLE IF NOT EXISTS [angles](ALPHA FLOAT);"""

        for j in range(1,16):
            sql+= sformat("""INSERT INTO [angles](ALPHA) VALUES({alpha});""",{"alpha":j*22.5})

        sql+="""
        CREATE TEMP TABLE [{temptable}](Geometry GEOMETRY,ALPHA FLOAT);
        INSERT OR REPLACE INTO [{temptable}] (Geometry,ALPHA)
        SELECT RotateCoords(Geometry,ALPHA),ALPHA
        FROM [{tablename}],[angles];
        ALTER TABLE [{tablename}] ADD COLUMN [ALPHA] FLOAT DEFAULT 0.0;
        INSERT INTO [{tablename}](Geometry,Alpha) SELECT * FROM [{temptable}];
        """
        db.execute(sql,env)



if __name__ == "__main__":

    chdir(r"D:\Users\vlr20\Projects\GitHub\gecosistema_feflow\gecosistema_feflow")
    db = SpatialDB("feflow.sqlite")
    db.attach("sicura.sqlite")

    sql = """
    SELECT AsBinary(MakePoint(L.X,L.Y)) as Shape,
           Ts.VALUE
           FROM sicura.[Ts] Ts
    INNER JOIN sicura.[location] L ON L.id=Ts.location_id
    WHERE Date='2015-08-01'
        AND Ts.type_id='176'
           AND Ts.location_id>1000000
           AND NOT L.X IS NULL
           AND NOT Ts.value IS NULL
           AND NOT L.point IN ('I1','I2');"""
    db.toShp(sql,None,"test.shp")



    db.close()


