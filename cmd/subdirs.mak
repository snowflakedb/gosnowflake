# Group makefile for cmd directories that contain example subdirectories.
# Set SUBDIRS before including this file. Each subdirectory must provide
# the install, run, lint, and fmt targets.

TARGETS := install run lint fmt

$(TARGETS): subdirs

subdirs: $(SUBDIRS)

$(SUBDIRS):
	@$(MAKE) -C $@ $(filter $(TARGETS),$(MAKECMDGOALS))

.PHONY: subdirs $(TARGETS) $(SUBDIRS)
