#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
int main() {
	int file1 = open("dd",O_EXCL|O_CREAT);
	int _errno = errno;
	printf("%d\n", _errno);
	close(file1);
}
