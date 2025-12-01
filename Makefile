.PHONY: install
install:
	poetry install

.PHONY: update
update:
	poetry run update_data

.PHONY: update-dev
update-dev:
	DEV_RUN="true" poetry run python ./scripts/update_data.py

.PHONY: clean
clean:
	rm -f data/daily_energy_mix_latest.json

# .PHONY: copy-local
# copy-local:
# 	cp public/data/daily_energy_mix_latest.json ../dailygrid.dev/src/data/daily_energy_mix_latest.json