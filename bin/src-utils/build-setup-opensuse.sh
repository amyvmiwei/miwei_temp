#!/usr/bin/env bash

arch=`uname -m`

if [ $arch == "i386" ] || [ $arch == "i586" ] || [ $arch == "i686" ] ; then
  ARCH=32
elif [ $arch == "x86_64" ] ; then
  ARCH=64
else
  echo "Unknown processor architecture: $arch"
  exit 1
fi

zypper --non-interactive install wget unzip gcc-c++ make cmake python-devel zlib-devel libexpat-devel libbz2-devel rrdtool rrdtool-devel ruby ruby-devel expat

# Boost
cd ~
wget http://downloads.sourceforge.net/boost/boost_1_44_0.tar.bz2
tar xjvf boost_1_44_0.tar.bz2
cd boost_1_44_0
./bootstrap.sh --with-libraries=filesystem,iostreams,program_options,system,thread,graph,regex
./bjam install
cd ~; /bin/rm -rf ~/boost_1_44_0*

zypper --non-interactive install git-core libevent-devel readline-devel ncurses-devel libpng-devel libart_lgpl-devel

# Cronolog
cd ~
wget http://cronolog.org/download/cronolog-1.6.2.tar.gz
tar xzvf cronolog-1.6.2.tar.gz 
cd cronolog-1.6.2/
./configure; make; make install
cd ~; /bin/rm -rf cronolog-1.6.2*

# SIGAR
cd ~
wget http://www.hypertable.org/pub/hyperic-sigar-1.6.4.zip
unzip hyperic-sigar-1.6.4.zip
cp hyperic-sigar-1.6.4/sigar-bin/include/*.h /usr/local/include
if [ $ARCH -eq 32 ]; then
  cp hyperic-sigar-1.6.4/sigar-bin/lib/libsigar-x86-linux.so /usr/local/lib
else
  cp hyperic-sigar-1.6.4/sigar-bin/lib/libsigar-amd64-linux.so /usr/local/lib
fi
/bin/rm -rf ~/hyperic-sigar-1.6.4*

# BerkeleyDB
cd ~
wget http://www.hypertable.org/pub/db-4.8.26.tar.gz
tar -xzvf db-4.8.26.tar.gz
cd db-4.8.26/build_unix/
../dist/configure --enable-cxx
make
make install
sh -c "echo '/usr/local/BerkeleyDB.4.8/lib' > /etc/ld.so.conf.d/BerkeleyDB.4.8.conf"
cd ~; /bin/rm -rf ~/db-4.8.26*

# Google RE2
cd ~
wget http://hypertable.org/pub/re2.tgz
tar -zxvf re2.tgz
cd re2
make
make install
/bin/rm -rf ~/re2*

# libunwind
if [ $ARCH -eq 64 ]; then
  cd ~
  wget http://download.savannah.gnu.org/releases/libunwind/libunwind-0.99-beta.tar.gz
  tar xzvf libunwind-0.99-beta.tar.gz
  cd libunwind-0.99-beta
  ./configure
  make
  make install
  cd ~
  /bin/rm -rf ~/libunwind-0.99-beta*
fi

# Google Perftools
cd ~
wget http://google-perftools.googlecode.com/files/google-perftools-1.8.3.tar.gz
tar xzvf google-perftools-1.8.3.tar.gz
cd google-perftools-1.8.3
./configure
make
make install
cd ~
/bin/rm -rf ~/google-perftools-1.8.3*

# Google Snappy
cd ~
wget http://snappy.googlecode.com/files/snappy-1.0.4.tar.gz
tar xzvf snappy-1.0.4.tar.gz
cd snappy-1.0.4
./configure
make
make install
cd ~
/bin/rm -rf ~/snappy-1.0.4*

# Java JDK
cd ~
if [ $ARCH -eq 32 ]; then
  arch="i586"
else
  arch="x64"
fi
wget -O jdk-6u25-linux-$arch-rpm.bin http://download.oracle.com/otn-pub/java/jdk/6u25-b06/jdk-6u25-linux-$arch-rpm.bin
chmod 755 jdk-6u25-linux-$arch-rpm.bin 
./jdk-6u25-linux-$arch-rpm.bin
rm -rf ~/jdk-6u25-linux-$arch-rpm*

update-alternatives --install /usr/bin/java java /usr/java/jdk1.6.0_25/bin/java 2000
update-alternatives --install /usr/bin/javac javac /usr/java/jdk1.6.0_25/bin/javac 2000

# Apache Ant
cd ~
wget http://apache.deathculture.net//ant/binaries/apache-ant-1.8.2-bin.zip
cd /usr/share
unzip ~/apache-ant-1.8.2-bin.zip
mv ant ant.old
ln -s apache-ant-1.8.2 ant
rm -rf /usr/java/default/ant*
cp apache-ant-1.8.2/lib/*.jar /usr/java/default
rm -rf ~/apache-ant-1.8.2-bin*

export PATH=$PATH:/usr/share/ant/bin

zypper --non-interactive install ant automake libtool flex bison pkg-config libevent-devel perl-Bit-Vector php5 php5-devel openssl libopenssl-devel rubygems perl-Class-Accessor

# Thrift
cd /usr/src
wget http://www.hypertable.org/pub/thrift-0.7.0.tar.gz
tar xzvf thrift-0.7.0.tar.gz
rm -f thrift
ln -s thrift-0.7.0 thrift
cd thrift-0.7.0
chmod 755 ./configure ./lib/php/src/ext/thrift_protocol/build/shtool
./configure
make
make install

zypper --non-interactive install dstat doxygen rrdtool graphviz gdb emacs-nox 

gem install capistrano sinatra rack thin json titleize

/sbin/ldconfig
