# Makefile for the Minibase project.  Needs GNU make.
#
# Warning: make depend overwrites this file.

.PHONY: depend clean backup setup

MAIN=hfpage

MINIBASE=..

CC=g++

CFLAGS= -DUNIX -Wall -g 

INCLUDES = -I${MINIBASE}/include -I.

#LFLAGS= -L${MINIBASE}/lib -liberty -lm
LFLAGS= -L${MINIBASE}/lib -lm -lbm -ldb

SRCS = main.C hfpage.C hfp_driver.C test_driver.C \
		new_error.C page.C system_defs.C

OBJS = $(SRCS:.C=.o)

$(MAIN):  $(OBJS)
	 $(CC) $(CFLAGS) $(INCLUDES) $(OBJS) -o $(MAIN) $(LFLAGS)

.C.o:
	$(CC) $(CFLAGS) $(INCLUDES) $(LFLAGS) -c $<

depend: $(SRCS)
	makedepend $(INCLUDES) $^

clean:
	rm -f *.o *~ $(MAIN) $(MAKECLEANGARBAGE)

backup:
	mkdir bak
	cp Makefile *.[Ch] bak

# Grab the sources for a user who has only the makefile
setup:
	/bin/cp -i $(MINIBASE)/src/*.[Ch] .
	-/bin/cp -i $(MINIBASE)/src/README .

# DO NOT DELETE THIS LINE -- make depend needs it

main.o: /usr/include/linux/stddef.h
main.o: /usr/include/stdlib.h /usr/include/alloca.h
main.o: ../include/hfp_driver.h ../include/test_driver.h ../include/minirel.h
main.o: ../include/da_types.h ../include/new_error.h /usr/include/assert.h
main.o: ../include/system_defs.h
hfpage.o: /usr/include/linux/stddef.h
hfpage.o: /usr/include/stdlib.h /usr/include/alloca.h
hfpage.o: /usr/include/memory.h /usr/include/string.h ../include/hfpage.h
hfpage.o: ../include/minirel.h ../include/da_types.h ../include/new_error.h
hfpage.o: /usr/include/assert.h ../include/system_defs.h ../include/page.h
hfpage.o: ../include/heapfile.h ../include/buf.h ../include/db.h
hfp_driver.o: /usr/include/linux/stddef.h
hfp_driver.o: /usr/lib/gcc/x86_64-linux-gnu/4.6/include/stdarg.h
hfp_driver.o: /usr/include/linux/types.h
hfp_driver.o: /usr/include/stdio.h /usr/include/stdlib.h
hfp_driver.o: /usr/include/stdlib.h /usr/include/alloca.h
hfp_driver.o: /usr/include/unistd.h
hfp_driver.o: /usr/include/features.h /usr/include/string.h
hfp_driver.o: /usr/include/assert.h ../include/db.h ../include/page.h
hfp_driver.o: ../include/minirel.h ../include/da_types.h
hfp_driver.o: ../include/new_error.h ../include/system_defs.h
hfp_driver.o: ../include/scan.h ../include/hfp_driver.h
hfp_driver.o: ../include/test_driver.h ../include/buf.h ../include/hfpage.h
test_driver.o: /usr/include/linux/stddef.h
test_driver.o: /usr/include/stdlib.h /usr/include/alloca.h
test_driver.o: /usr/include/stdio.h
test_driver.o: /usr/lib/gcc/x86_64-linux-gnu/4.6/include/stdarg.h
test_driver.o: /usr/include/linux/types.h
test_driver.o: /usr/include/stdio.h /usr/include/string.h
test_driver.o: /usr/include/errno.h
test_driver.o: /usr/include/assert.h ../include/new_error.h
test_driver.o: ../include/test_driver.h ../include/minirel.h
test_driver.o: ../include/da_types.h ../include/system_defs.h
test_driver.o: /usr/include/pwd.h /usr/include/unistd.h
test_driver.o: /usr/include/unistd.h /usr/include/features.h
new_error.o: ../include/new_error.h /usr/include/assert.h
new_error.o: /usr/include/linux/stddef.h
new_error.o: /usr/include/stdio.h
new_error.o: /usr/lib/gcc/x86_64-linux-gnu/4.6/include/stdarg.h
new_error.o: /usr/include/linux/types.h
new_error.o: /usr/include/stdio.h /usr/include/stdlib.h
new_error.o: /usr/include/stdlib.h /usr/include/alloca.h
page.o: ../include/page.h ../include/minirel.h ../include/da_types.h
system_defs.o: /usr/include/linux/stddef.h
system_defs.o: /usr/lib/gcc/x86_64-linux-gnu/4.6/include/stdarg.h
system_defs.o: /usr/include/linux/types.h
system_defs.o: /usr/include/stdio.h ../include/minirel.h
system_defs.o: ../include/da_types.h ../include/new_error.h
system_defs.o: /usr/include/assert.h ../include/system_defs.h ../include/db.h
system_defs.o: /usr/include/string.h /usr/include/stdlib.h
system_defs.o: /usr/include/stdlib.h /usr/include/alloca.h
system_defs.o: ../include/page.h ../include/buf.h
