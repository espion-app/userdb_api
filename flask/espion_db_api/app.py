import datetime
import json
import logging
import re
import uuid

from dateutil.parser import parse as parse_dt
from flask import Blueprint, request, current_app as app, abort
from sqlalchemy import Table, MetaData, select, func, or_, and_
from sqlalchemy.exc import DatabaseError

from flask_suite.builder import AppBuilder
from flask_suite.jsonx import jsonxfy

api_blueprint = Blueprint('api', __name__)

# curl --data-binary '{"items":[{"id":"obj://1","__ttl__":86400,"title":"My object"}]}' -H "Content-Type: application/json" http://localhost:5000/record_items -vvv
# curl --data-binary '{"type":"","ids":["obj://1","obj://2"]}' -H "Content-Type:application/json" http://localhost:5000/db?method=items_exist -vvv
# curl "http://localhost:5000/db?method=item_exists&id=obj://1"

@api_blueprint.route('/db', methods=['GET','POST'])
def dispatch():
	method = request.args.get('method')
	if method == 'record_items':
		if request.method != 'POST':
			return abort(405)
		return record_items()
	if method == 'items_exist':
		if request.method != 'POST':
			return abort(405)
		return items_exist()
	if method == 'item_exists':
		return item_exists()
	if method == 'query':
		return query()
	abort(404)

@api_blueprint.route('/record_items', methods=['POST'])
def record_items():
	items = request.json.get('items')
	messages = []

	if not isinstance(items, list):
		return jsonxfy(status='error', messages=['Invalid JSON body'])

	for i, item in enumerate(items):
		# ID
		try:
			id = item['id']
		except KeyError:
			messages.append('Object %i: missing id' % i)
			continue

		# Type
		type = item.get('type')
		if type is None:
			type = u''
		if not isinstance(type, unicode):
			type = unicode(type)

		# Partial update
		partial_update = item.get('__partial_update__', False)

		# Datetime
		dt = item.get('__dt__') or item.get('__visited_on__')
		if dt is None or not isinstance(dt, basestring):
			dt = datetime.datetime.utcnow().isoformat()

		# TTL
		ttl = item.get('__ttl__')
		if ttl is not None and isinstance(ttl, (int, float, long)):
			expires_on = (datetime.datetime.utcnow()
			              + datetime.timedelta(seconds=ttl))
		else:
			expires_on = (datetime.datetime.utcnow()
			              + app.config.get('DEFAULT_TTL',
			                               datetime.timedelta(days=14)))

		# Launch a transaction, retry in case of duplicate errors
		while 1:
			with app.db.begin_nested() as trans:
				# Get the original row
				row = app.db.execute(app.table.select(
					((app.table.c.id == id)
					 & (app.table.c.type == type)), for_update=True)).fetchone()
				if row is None:
					obj = {
						'uuid': str(uuid.uuid4()),
						'type': type,
						'id': id,
						'first_seen_on': dt,
						'attributes': {},
						'history': {}
					}
				else:
					obj = dict(row.items())

				obj['last_seen_on'] = dt
				obj['expires_on'] = expires_on
				obj['active'] = True
				if not partial_update:
					obj['fully_updated_on'] = dt

				if obj['attributes'] is None:
					obj['attributes'] = {}
				if obj['history'] is None:
					obj['history'] = {}

				for k,v in item.iteritems():
					if k in ('id','type') or k.startswith('__'):
						continue
					history = k.startswith('$')
					if history:
						k = k[1:]
						try:
							attr_hist = obj['history'][k][:]
						except KeyError:
							attr_hist = obj['history'][k] = []

						insert_index = 0
						for i, (h_dt, h_val) in enumerate(attr_hist):
							if h_dt > dt:
								insert_index = i
								break
						else:
							insert_index = len(attr_hist)

						if insert_index > 0:
							prev_dt, prev_val = attr_hist[insert_index - 1]
							if prev_val == v:
								# Don't insert, the previous value is identical
								insert_index = None

						if insert_index is not None and insert_index < len(attr_hist):
							next_dt, next_val = attr_hist[insert_index]
							if next_val == v:
								insert_index = None

						if insert_index is not None:
							attr_hist.insert(insert_index, (dt, v))
						obj['history'][k] = attr_hist
						v = attr_hist[-1][1]
					obj['attributes'][k] = v

				jsonified = dict(obj)
				jsonified['attributes'] = json.dumps(obj['attributes'])
				jsonified['history'] = json.dumps(obj['history'])

				if row is None:
					try:
						app.db.execute(app.table.insert(obj))
					except DatabaseError, e:
						if is_duplicate_error(e):
							trans.rollback()
							continue
						raise
				else:
					app.db.execute(app.table.update(
					    whereclause=(app.table.c.uuid == row['uuid']),
					    values=obj))
			break

	return jsonxfy(status='ok', messages=messages)

def _items_exist(type, ids_list, updated_after_str):
	if not isinstance(ids_list, list):
		return abort(400)
	ids = map(unicode, ids_list)
	res = dict((id, False) for id in ids)

	where = app.table.c.id.in_(ids)
	if updated_after_str:
		where &= (app.table.fully_updated_on > parse_dt(updated_after_str))

	res.update(r for r in app.db.execute(select([app.table.c.id, True], where)))
	return res

@api_blueprint.route('/items_exist', methods=['POST'])
def items_exist():
	params = request.json
	type = unicode(params.get('type', ''))
	updated_after_str = params.get('updated_after')
	ids_list = params.get('ids', [])
	res = _items_exist(type, ids_list, updated_after_str)
	return jsonxfy(status='ok', exist=res)

@api_blueprint.route('/item_exists')
def item_exists():
	type = unicode(request.args.get('type', ''))
	updated_after_str = request.args.get('updated_after')
	id = request.args.get('id')
	if id is None:
		return abort(405)
	res = _items_exist(type, [id], updated_after_str)
	return jsonxfy(status='ok', exists=res[id])

@api_blueprint.route('/query')
def query():
	filters = []

	active_str = request.args.get('$active', '')
	if active_str:
		if active_str.lower() in ('1','t','true'):
			filters.append(app.table.c.expires_on >= datetime.datetime.utcnow())
		else:
			filters.append(app.table.c.expires_on < datetime.datetime.utcnow())

	for t in ('before','after'):
		arg_str = request.args.get('$seen_' + t)
		if arg_str:
			dt = parse_dt(arg_str)
			if t == 'before':
				filters.append(app.table.c.first_seen_on < dt)
			else:
				filters.append(app.table.c.last_seen_on > dt)



	for key in ('id', 'type'):
		in_key = key + '[]'
		if in_key in request.args:
			vals = request.args.getlist(in_key)
			filters.append(getattr(app.table.c, key).in_(vals))

	for in_key, vals in request.args.iterlists():
		if not in_key.endswith('[]') or not vals:
			continue
		fieldname = in_key[:-2]
		if fieldname in ('id', 'type'):
			continue
		ored_filters = []
		for val in vals:
			ored_filters.append(app.table.c.attributes.op('->>')(fieldname) == unicode(val))
		if ored_filters:
			filters.append(reduce(or_, ored_filters))


	items = []
	if filters:
		where = reduce(and_, filters)
	else:
		where = None

	for row in app.db.execute(select([app.table], where)):
		item = {}
		items.append(item)
		for key in ('id', 'type', 'attributes'):
			item[key] = row[key]

	return jsonxfy(status='ok', items=items)

class EspionDBAPIBuilder(AppBuilder):

	def setup_blueprints(self, app):
		app.register_blueprint(api_blueprint, url_prefix='/' + app.config.get('API_PATH_PREFIX', '').strip('/'))

		@app.route('/error')
		def test_error():
			raise ValueError('Exc')

	def setup_db(self, app):
		AppBuilder.setup_db(self, app)
		app.table = Table(app.config.get('TABLE_NAME', 'items'), MetaData(),
		                   autoload=True, autoload_with=app.db.bind)

zx_error_re = re.compile('.*\[SQLState: ([A-F0-9]{5})\].*')
def is_duplicate_error(exc):
	if not isinstance(exc, DatabaseError):
		return False
	module_name = exc.orig.__module__
	if module_name == 'pg8000.errors':
		return exc.orig.args[1] == '23505'
	elif module_name == 'psycopg2':
		return exc.orig.pgcode  == '23505'
	elif module_name == 'zxJDBC':
		m = zx_error_re.match(exc.orig.message.splitlines()[-1])
		return m.group(1) == '23505'
	raise NotImplementedError('This function is not compatible with %s' %
	                          module_name)

if __name__ == '__main__':
	EspionDBAPIBuilder(__name__).main()
