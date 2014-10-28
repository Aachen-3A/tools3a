wget http://www.hepforge.org/archive/lhapdf/LHAPDF-6.0.5.tar.gz
tar -xf LHAPDF-6.0.5.tar.gz

wget http://yaml-cpp.googlecode.com/files/yaml-cpp-0.3.0.tar.gz -O- | tar xz
cd yaml-cpp
cmake -DBUILD_SHARED_LIBS=ON -DCMAKE_INSTALL_PREFIX=$PWD/../local
make -j2 && make install
cd ..

cd LHAPDF-6.0.5

./configure --with-boost=/cvmfs/cms.cern.ch/slc5_amd64_gcc462/external/boost/1.51.0/ --prefix=$PWD/../local --with-yaml-cpp=$PWD/../local

make -j8
make install

cd ..
cd local/share/LHAPDF/
wget http://www.hepforge.org/archive/lhapdf/pdfsets/6.0.5/CT10.tar.gz
tar -xf CT10.tar.gz
wget http://www.hepforge.org/archive/lhapdf/pdfsets/6.0.5/unvalidated/CT10as.tar.gz
tar -xf CT10as.tar.gz
wget http://www.hepforge.org/archive/lhapdf/pdfsets/6.0.5/MSTW2008nlo68cl.tar.gz
tar -xf MSTW2008nlo68cl.tar.gz
wget http://www.hepforge.org/archive/lhapdf/pdfsets/6.0.5/MSTW2008nlo68cl_asmz+68cl.tar.gz
tar -xf MSTW2008nlo68cl_asmz+68cl.tar.gz
wget http://www.hepforge.org/archive/lhapdf/pdfsets/6.0.5/MSTW2008nlo68cl_asmz-68cl.tar.gz
tar -xf MSTW2008nlo68cl_asmz-68cl.tar.gz
wget http://www.hepforge.org/archive/lhapdf/pdfsets/6.0.5/NNPDF23_nlo_as_0116.tar.gz
tar -xf NNPDF23_nlo_as_0116.tar.gz
wget http://www.hepforge.org/archive/lhapdf/pdfsets/6.0.5/NNPDF23_nlo_as_0117.tar.gz
tar -xf NNPDF23_nlo_as_0117.tar.gz
wget http://www.hepforge.org/archive/lhapdf/pdfsets/6.0.5/NNPDF23_nlo_as_0118.tar.gz
tar -xf NNPDF23_nlo_as_0118.tar.gz
wget http://www.hepforge.org/archive/lhapdf/pdfsets/6.0.5/NNPDF23_nlo_as_0119.tar.gz
tar -xf NNPDF23_nlo_as_0119.tar.gz
wget http://www.hepforge.org/archive/lhapdf/pdfsets/6.0.5/NNPDF23_nlo_as_0120.tar.gz
tar -xf NNPDF23_nlo_as_0120.tar.gz
wget http://www.hepforge.org/archive/lhapdf/pdfsets/6.0.5/NNPDF23_nlo_as_0121.tar.gz
tar -xf NNPDF23_nlo_as_0121.tar.gz
wget http://www.hepforge.org/archive/lhapdf/pdfsets/6.0.5/NNPDF23_nlo_as_0122.tar.gz
tar -xf NNPDF23_nlo_as_0122.tar.gz
wget http://www.hepforge.org/archive/lhapdf/pdfsets/6.0.5/cteq6l1.tar.gz
tar -xf cteq6l1.tar.gz

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PWD/local/lib/
export LHAPDF_BASE=$PWD/local/
