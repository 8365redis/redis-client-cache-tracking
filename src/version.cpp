#include <string>
#include <iostream>
#include "version.h"
int Get_Module_Version(const char* version_string) {
    int major = 0;
    int minor = 0;
    int patch = 0;
    sscanf(version_string, "%d.%d.%d", &major, &minor, &patch);
    return (10000 * major + 100 * minor + patch);
}