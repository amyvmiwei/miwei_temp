#include "Common/Compat.h"
#include "Common/Init.h"
#include "Common/Filesystem.h"

using namespace Hypertable;


int main(int argc, char *argv[]) 
{
  const char *paths[] = {
    "test.real", "/usr/bin/perl", "/usr/lib", "/usr/", "usr",
    "/", ".", "..", "/test/path/", "/test/path/2//", "test/path/3///",
    0
  };

  const char *basenames[] = {
    "test.real", "perl", "lib", "usr", "usr", 
    "/", ".", "..", "path", "2", "3", 
    0
  };

  const char *dirnames[] = {
    ".", "/usr/bin", "/usr", "/", ".", 
    "/", ".", ".", "/test", "/test/path", "test/path", 
    0
  };

  int i=0;
  for (const char **p=&paths[0]; *p; ++p, ++i) {
    String b=Filesystem::basename(*p);
    String d=Filesystem::dirname(*p);
    HT_ASSERT(b==basenames[i]);
    HT_ASSERT(d==dirnames[i]);
    
  }

  return (0);
}
