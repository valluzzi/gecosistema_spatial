# -------------------------------------------------------------------------------
# Licence:
# Copyright (c) 2012-2018 Luzzi Valerio 
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
# Name:        getvalueat.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     03/12/2018
# -------------------------------------------------------------------------------
import gdal,gdalconst

def MapToPixel(mx, my, gt):
    """
    MapToPixel
    """
    ''' Convert map to pixel coordinates
        @param  mx:    Input map x coordinate (double)
        @param  my:    Input map y coordinate (double)
        @param  gt:    Input geotransform (six doubles)
        @return: px,py Output coordinates (two ints)
    '''
    if gt[2] + gt[4] == 0:  # Simple calc, no inversion required
        px = (mx - gt[0]) / gt[1]
        py = (my - gt[3]) / gt[5]

        return int(px), int(py)

    raise Exception("I need to Invert geotransform!")


def GetValueAt(X, Y, filename, band=1):
    """
    GetValueAt
    """
    # Converto in epsg 3857
    dataset = gdal.Open(filename, gdalconst.GA_ReadOnly)
    if dataset:
        band = dataset.GetRasterBand(band)
        cols = dataset.RasterXSize
        rows = dataset.RasterYSize
        gt = dataset.GetGeoTransform()
        # Convert from map to pixel coordinates.
        # Only works for geotransforms with no rotation.
        # If raster is rotated, see http://code.google.com/p/metageta/source/browse/trunk/metageta/geometry.py#493
        # print  gt
        j, i = MapToPixel(float(X), float(Y), gt)

        if i in range(0, band.YSize) and j in range(0, band.XSize):
            scanline = band.ReadRaster(j, i, 1, 1, buf_type=gdalconst.GDT_Float32)  # Assumes 16 bit int aka 'short'
            (value,) = struct.unpack('f', scanline)
            return value

    # raise ValueError("Unexpected (Lon,Lat) values.")
    return None