#!/usr/bin/env bash

conda create -n env_hca_live -y -c conda-forge python=$PYTHON_VERSION numpy=1.14.1 pandas=0.22.0 scipy=1.0.0 libgfortran=3.0 mkl-service pymc3=3.5 lightgbm=2.2.0 scikit-optimize=0.5.2 scikit-learn catboost pip
source activate env_hca_live
python -m pip install -r requirements_latest.txt --no-cache-dir
python -m pip install -r requirements_blaze_latest.txt --no-cache-dir
pip install cvxpy==1.0.9 --no-cache-dir

#orig pip install zipline==1.3.0 --no-cache-dir
cd ~/hca/zipline
python -m pip install -r etc/requirements_dev.txt -r etc/requirements_blaze.txt
python -m pip install -e .[ib] --no-cache-dir

cd ~/hca/alphatools
python -m pip install statsmodels==0.9.0 --upgrade --no-cache-dir
python -m pip install alphalens==0.3.2 --no-cache-dir
python -m pip install pyfolio --no-cache-dir
python -m pip install ipykernel --no-cache-dir
python -m pip install graphviz==0.9
#conda install -y -c pytorch pytorch-nightly-cpu
#conda install -y -c fastai torchvision-nightly-cpu
#conda install -y -c fastai fastai

cd ..
#python -m pip install -e alphatools --no-cache-dir

python -m ipykernel install --user --name env_hca_live --display-name "env_hca_live"

source ~/.bashrc
# must append to .bashrc
if [ "$MKL_THREADING_LAYER" = "" ]
then
    export MKL_THREADING_LAYER=GNU
    echo 'export MKL_THREADING_LAYER=GNU' >> ~/.bashrc
    echo 'Please source the .bashrc file to activate MKL_THREADING env variable.'
fi

if [ "$THEANO_FLAGS" = "" ]
then
    # Needed for Mac OS X
    export "THEANO_FLAGS='gcc.cxxflags=-Wno-c++11-narrowing'"
    echo "export \"THEANO_FLAGS='gcc.cxxflags=-Wno-c++11-narrowing'\"" >> ~/.bashrc
    echo 'Please source the .bashrc file to activate the THEANO_FLAGS env variable.'
fi
