#include<iostream>
#include<vector>
#include<map>
#include<algorithm>
#include<utility>
#include<string>
#include <stdio.h> 
#include <stdlib.h> 
#include<time.h> 
using namespace std;

uint16_t recordSize[] = { 74,100,120,140 };
uint8_t recContent[] = { 'a','b','c','d' };

#define HEADER_SIZE 4

uint16_t gernerateLength() {
	uint8_t index = rand() % 4;
	return index;
}

uint8_t* getHeader(const uint8_t index, uint8_t *header) {
	uint16_t size = recordSize[index];

	uint8_t* b = (uint8_t*)&size;
	header[1] = *b;
	b++;
	header[0] = *b;
	return header;
}

int getdata(const uint8_t index, uint8_t *data) {
	uint16_t size = recordSize[index] - HEADER_SIZE;
	uint8_t byte = recContent[index];

	for (int i = 0; i < size; i++) {
		data[i] = byte;
	}
	return size;
}

int main() 

{
	srand(time(0));
	FILE *f = fopen("test100Rec.bin", "wb+");
	FILE *f1 = fopen("test100Rec_meta.bin", "wt+");
	uint8_t data[1024] = { 0 };
	uint8_t header[HEADER_SIZE] = { 0 };

	if (f != nullptr) {
		int cnt = 0;
		while (true) {
			uint16_t index = gernerateLength();
			uint16_t size = recordSize[index];
			
			uint8_t* h = getHeader(index, header);
			int bsize = getdata(index, data);

			fwrite(h, 4, 1, f);
			fwrite(data, bsize, 1, f);
			fflush(f);

			fprintf(f1, "%d-%c\n", size, recContent[index]);
			fflush(f1);
			cnt++;

			if (cnt == 100)
				break;
		}
	}
	return 0;


}