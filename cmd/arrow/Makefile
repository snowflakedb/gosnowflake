SUBDIRS := batches transform_batches_to_rows
TARGETS := all install run lint fmt

$(TARGETS): subdirs

subdirs: $(SUBDIRS)

$(SUBDIRS):
	@$(MAKE) -C $@ $(filter $(TARGETS),$(MAKECMDGOALS))

.PHONY: subdirs $(TARGETS) $(SUBDIRS)
