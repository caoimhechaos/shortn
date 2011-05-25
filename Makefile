include $(GOROOT)/src/Make.inc

TARG=	shortn
GOFILES=\
	db.go \
	shortn.go

include $(GOROOT)/src/Make.cmd
