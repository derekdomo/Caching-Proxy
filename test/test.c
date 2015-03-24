#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
int main() {
	int file1 = open("dd",O_EXCL|O_CREAT);
	int _erro = errno;
	printf("%d\n", _erro);
	sleep(5);
	close(file1);
}
