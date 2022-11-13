//http://stackoverflow.com/questions/7666509/hash-function-for-string
unsigned long hash(unsigned char *name)
{
	unsigned long h = 2166136261;

	while (*name) {
		h = (h ^ *name++) * 16777619;
	}

	return (h >> 1); // To avoid negative values when casting to signed integer.
}
