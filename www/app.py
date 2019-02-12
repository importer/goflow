import os

import bleach as bleach
from flask import Flask, url_for, redirect, render_template, request, Markup
from flask_sqlalchemy import SQLAlchemy
import json

import flask_admin as admin
import flask_login as login
from flask_admin.contrib import sqla
from flask_admin import helpers, expose
from werkzeug.security import generate_password_hash, check_password_hash
from wtforms import form, fields, validators
# from flask_admin.contrib import rediscli
# from redis import Redis
import sys
from functools import reduce
from datetime import datetime, timedelta
from flask_admin.actions import action
from flask_admin.contrib.sqla import filters as sqla_filters, tools
from flask_admin.babel import gettext, ngettext, lazy_gettext
from flask import flash

sys.path.append('../')
import models
from utils import db
from state import State

# Create Flask application
app = Flask(__name__)

# Create dummy secrey key so we can use sessions
app.config['SECRET_KEY'] = '123456790'

# Create in-memory database
app.config['DATABASE_FILE'] = 'sample_db.sqlite'
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:root@localhost:3306/cflow?charset=utf8'
app.config['SQLALCHEMY_ECHO'] = True
db.configure_orm(app.config['SQLALCHEMY_DATABASE_URI'])
db = SQLAlchemy(app)


# Initialize flask-login
def init_login():
    login_manager = login.LoginManager()
    login_manager.init_app(app)

    # Create user loader function
    @login_manager.user_loader
    def load_user(user_id):
        return db.session.query(models.User).get(user_id)


# Define login and registration forms (for flask-login)
class LoginForm(form.Form):
    login = fields.StringField(validators=[validators.required()])
    password = fields.PasswordField(validators=[validators.required()])

    def validate_login(self, field):
        user = self.get_user()

        if user is None:
            raise validators.ValidationError('Invalid user')

        # we're comparing the plaintext pw with the the hash from the db
        if not check_password_hash(user.password, self.password.data):
            # to compare plain text passwords use
            # if user.password != self.password.data:
            raise validators.ValidationError('Invalid password')

    def get_user(self):
        return db.session.query(models.User).filter_by(login=self.login.data).first()


class RegistrationForm(form.Form):
    login = fields.StringField(validators=[validators.required()])
    email = fields.StringField()
    password = fields.PasswordField(validators=[validators.required()])

    def validate_login(self, field):
        if db.session.query(models.User).filter_by(login=self.login.data).count() > 0:
            raise validators.ValidationError('Duplicate username')


# Create customized model view class
class MyModelView(sqla.ModelView):

    def is_accessible(self):
        return login.current_user.is_authenticated


#
class TaskDefineView(sqla.ModelView):
    column_searchable_list = ['task_id', 'task_name', 'command']
    column_filters = ['enable']
    column_editable_list = ['task_name', 'command', 'enable', 'run_type']
    form_choices = {'run_type': [
        ('single', 'single'),
        ('interval', 'interval'),
        ('hour', 'hour'),
        ('day', 'day'),
        ('month', 'month'),
    ], }

    def is_accessible(self):
        return login.current_user.is_authenticated


def log_run_link(v, c, m, p):
    log_id = m.id
    url = url_for(
        'admin.task_log_view',
        log_id=log_id)
    return Markup('<a href="{url}">{m.id}</a>'.format(**locals()))


# TaskInstanceView
class TaskInstanceLogView(sqla.ModelView):
    column_searchable_list = ['log_id', ]
    column_filters = ['in_time']
    form_excluded_columns = ['stdout', ]

    def is_accessible(self):
        return login.current_user.is_authenticated


# TaskInstanceView
class TaskInstanceView(sqla.ModelView):
    named_filter_urls = True
    can_create = False
    can_edit = False
    # can_delete = False
    column_display_pk = True

    column_searchable_list = ['task_id', 'name', 'command', 'hostname']
    column_filters = ['etl_day', 'status', 'task_type', 'scheduler_time']
    column_formatters = dict(
        id=log_run_link,
    )

    @action('rerun',
            lazy_gettext('重做当前'),
            lazy_gettext('Are you sure you want to ReRun selected jobs?'))
    def action_rerun(self, ids):
        try:
            query = tools.get_query_for_ids(self.get_query(), self.model, ids)
            count = 0
            for job in query:
                msg = models.Admin().rerun_task(job.task_id, job.etl_day,
                                                up_and_down=False,
                                                run_up=False,
                                                run_down=False,
                                                force=False)
                count += 1

            flash(ngettext('jobs was successfully rerun.',
                           '%(count)s records were successfully rerun.',
                           count,
                           count=count), 'success')
        except Exception as ex:
            if not self.handle_view_exception(ex):
                raise

            flash(gettext('Failed to rerun records. %(error)s', error=str(ex)), 'error')

    @action('rerunAfter',
            lazy_gettext('重做当前及后续'),
            lazy_gettext('Are you sure you want to ReRun selected jobs?'))
    def action_rerunafter(self, ids):
        try:
            query = tools.get_query_for_ids(self.get_query(), self.model, ids)
            count = 0
            for job in query:
                msg = models.Admin().rerun_task(job.task_id, job.etl_day,
                                                up_and_down=False,
                                                run_up=False,
                                                run_down=True,
                                                force=False)
                count += 1

            flash(ngettext('jobs was successfully rerun.',
                           '%(count)s records were successfully rerun.',
                           count,
                           count=count), 'success')
        except Exception as ex:
            if not self.handle_view_exception(ex):
                raise

            flash(gettext('Failed to rerun records. %(error)s', error=str(ex)), 'error')

    @action('resuccess',
            lazy_gettext('强制通过'),
            lazy_gettext('Are you sure you want to success selected jobs?'))
    def action_resuccess(self, ids):
        try:
            query = tools.get_query_for_ids(self.get_query(), self.model, ids)
            count = query.update({self.model.status, 'success'}, synchronize_session=False)

            flash(ngettext('jobs was successfully success.',
                           '%(count)s records were successfully success.',
                           count,
                           count=count), 'success')
        except Exception as ex:
            if not self.handle_view_exception(ex):
                raise

            flash(gettext('Failed to rerun records. %(error)s', error=str(ex)), 'error')

    @action('dryrun',
            lazy_gettext('强制执行'),
            lazy_gettext('Are you sure you want to dryrun selected jobs?'))
    def action_dryrun(self, ids):
        try:
            query = tools.get_query_for_ids(self.get_query(), self.model, ids)
            count = 0
            for job in query:
                msg = models.Admin().rerun_task(job.task_id, job.etl_day,
                                                up_and_down=False,
                                                run_up=False,
                                                run_down=False,
                                                force=True)
                count += 1

            flash(ngettext('jobs was successfully dryrun.',
                           '%(count)s records were successfully dryrun.',
                           count,
                           count=count), 'success')
        except Exception as ex:
            if not self.handle_view_exception(ex):
                raise

            flash(gettext('Failed to rerun records. %(error)s', error=str(ex)), 'error')

    def is_accessible(self):
        return login.current_user.is_authenticated


# CronView
class CronView(sqla.ModelView):
    # named_filter_urls = True
    can_create = False
    can_edit = False
    can_delete = False
    column_display_pk = True

    column_searchable_list = ['task_id', 'name', 'command']
    column_filters = ['etl_day', 'status', 'running_type']
    column_formatters = dict(
        id=log_run_link,
    )

    def is_accessible(self):
        return login.current_user.is_authenticated


#
class CronConfView(sqla.ModelView):
    column_searchable_list = ['name', 'task_id', 'command']
    column_filters = ['enable', 'type']
    column_editable_list = ['name', 'command', 'enable', 'start_time', 'redo_param', 'type', 'cron_type', 'modify_time']
    form_choices = {
        'enable': [
            ('enabled', 'enabled'),
            ('disabled', 'disabled'),
        ],
        'cron_type': [
            ('single', 'single'),
            ('interval', 'interval'),
            ('hour', 'hour'),
            ('day', 'day'),
            ('month', 'month'),
        ],
        'type': [
            ('task_cron', 'task_cron'),
            ('task_work', 'task_work'),
        ]

    }

    def is_accessible(self):
        return login.current_user.is_authenticated


# Create customized index view class that handles login & registration
class MyAdminIndexView(admin.AdminIndexView):

    @expose('/')
    def index(self):
        self._template = 'admin/index.html'
        self._template_args['etl_date'] = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        if not login.current_user.is_authenticated:
            return redirect(url_for('.login_view'))
        return super(MyAdminIndexView, self).index()

    @expose('/login/', methods=('GET', 'POST'))
    def login_view(self):
        # handle user login
        form = LoginForm(request.form)
        if helpers.validate_form_on_submit(form):
            user = form.get_user()
            login.login_user(user)

        if login.current_user.is_authenticated:
            return redirect(url_for('.index'))
        link = '<p>Don\'t have an account? <a href="' + url_for('.register_view') + '">Click here to register.</a></p>'
        self._template_args['form'] = form
        self._template_args['link'] = link
        self._template = 'admin/login.html'
        return super(MyAdminIndexView, self).index()

    @expose('/register/', methods=('GET', 'POST'))
    def register_view(self):
        form = RegistrationForm(request.form)
        if helpers.validate_form_on_submit(form):
            user = models.User()

            form.populate_obj(user)
            # we hash the users password to avoid saving it as plaintext in the db,
            # remove to use plain text:
            user.password = generate_password_hash(form.password.data)
            print(user.email)
            db.session.add(user)
            db.session.commit()

            login.login_user(user)
            return redirect(url_for('.index'))
        link = '<p>Already have an account? <a href="' + url_for('.login_view') + '">Click here to log in.</a></p>'
        self._template_args['form'] = form
        self._template_args['link'] = link
        return super(MyAdminIndexView, self).index()

    @expose('/logout/')
    def logout_view(self):
        login.logout_user()
        return redirect(url_for('.index'))

    @expose('/task_log/')
    def task_log_view(self):
        log_id = request.args.get("log_id")
        log_id = 1455 if log_id is None else log_id
        # login.logout_user()
        TI = db.session.query(models.TaskInstance).filter(models.TaskInstance.id == log_id).one()
        TI_log = db.session.query(models.TtaskInstanceLog).filter(models.TtaskInstanceLog.log_id == log_id).one()
        self._template_args['task_id'] = '''etl_day:{0}## task_id:{1}## name:{2}## task_type:{3}## status:{4}## 
                                            scheduler_time:{5}## begin_time:{6}## end_time:{7}##
                                            command:{8}## hostname{9}'''.format(TI.etl_day
                                                                                , TI.task_id
                                                                                , TI.name
                                                                                , TI.task_type
                                                                                , TI.status
                                                                                , TI.scheduler_time
                                                                                , TI.begin_time
                                                                                , TI.end_time
                                                                                , TI.command
                                                                                , TI.hostname)
        self._template_args['log_detail'] = TI_log.stdout
        self._template_args['in_time'] = TI_log.in_time
        return self.render('/admin/task_log.html')


# 字典去重
def list_dict_duplicate_removal(data_list):
    # data_list = [{"a": "123", "b": "321"}, {"a": "123", "b": "321"}, {"b": "321", "a": "123"}]
    run_function = lambda x, y: x if y in x else x + [y]
    return reduce(run_function, [[], ] + data_list)


# Flask views
@app.route('/')
def index():
    return render_template('demo2.html')


@app.route('/api/get_dependency', methods=['GET', 'POST'])
def get_dependency():
    data = request.get_data()
    # print(data.decode("utf-8"))
    # json_data = json.loads(data.decode("utf-8"))
    # print(json_data)
    # data={'state':[{ 'id': 1, 'label': 'V1##spark-sql', 'class': 'type-suss' },
    #             { 'id': 2, 'label': 'V2##python', 'class': 'type-suss' },
    #             { 'id': 3, 'label': 'V3##spark-sql', 'class': 'type-init' },
    #             { 'id': 4, 'label': 'V4##shell', 'class': 'type-ready' },
    #             { 'id': 5, 'label': 'V5##python', 'class': 'type-fail' }
    #             ],
    #     'edg':[{ 'start': 1, 'end': 4, },
    #             { 'start': 1, 'end': 3, },
    #             { 'start': 1, 'end': 2, },
    #             { 'start': 3, 'end': 2, },
    #             { 'start': 3, 'end': 4, },
    #             { 'start': 4, 'end': 5, }
    #     # 'edg':[   { 'start':'hour_demo01', 'end':'hours_task'},
    #     #         { 'start':'hour_demo02', 'end':'hours_task'},
    #     #         { 'start':'hour_demo03','end':'hours_task'},
    #     #         { 'start':'mon_demo01',  'end':'month_task'},
    #     #         { 'start':'mon_demo02',  'end':'month_task'}
    #                         ]}
    sql = '''
    select IFNULL(c.id,d.id) st_id,IFNULL(c.task_id,d.task_id) st_task_id,IFNULL(c.task_name,d.name) st_name,
    b.id,b.task_id,b.task_name from task_dependency a
    LEFT JOIN task_define b on a.task_id=b.task_id
    LEFT JOIN task_define c on a.dependency_task_id=c.task_id
    LEFT JOIN cron_conf d on a.dependency_task_id=d.task_id
    where IFNULL(c.id,d.id)<>b.id
    '''
    edg = []
    ed = {}
    st = {}
    state = []
    dependency = db.session.execute(sql).fetchall()
    for i in dependency:
        ed = {'start': i[0], 'end': i[3], }
        edg.append(ed)
        # ['mon_demo03']=id
        st[i[1]] = i[0]
        st[i[4]] = i[3]

    etl_day = (datetime.now() + timedelta(days=-1)).strftime('%Y%m%d')
    sql = """select task_id,max(`status`) from (select task_id,`status`,etl_day from task_instance  
            	union select task_id,`status`,etl_day from cron_log) a group by task_id"""
    # where etl_day like '{etl_day}%';""".format(etl_day=etl_day)
    task_status = db.session.execute(sql).fetchall()
    suss_counts = 0
    fail_counts = 0
    run_counts = 0

    for x, y in st.items():
        # print(x)
        for ts in task_status:
            if x == ts[0]:
                if ts[1] == 'success':
                    suss_counts += 1
                    class_type = 'type-suss'
                elif ts[1] == 'failed':
                    fail_counts += 1
                    class_type = 'type-fail'
                elif ts[1] == 'waiting_dep':
                    class_type = 'type-ready'
                elif ts[1] == 'queued':
                    class_type = 'type-queue'
                else:
                    class_type = 'type-run'
                    run_counts += 1
                st1 = {'id': y, 'label': x, 'class': class_type}
                state.append(st1)
    task_count = {}
    task_count['suss_counts'] = suss_counts
    task_count['fail_counts'] = fail_counts
    task_count['run_counts'] = run_counts
    data = {'state': state, 'edg': edg, 'task_count': task_count}
    return json.dumps(data)


# Initialize flask-login
init_login()

# Create admin
admin = admin.Admin(app, 'Goflow', index_view=MyAdminIndexView(), template_mode='bootstrap3',
                    base_template='admin/mybase.html')

# Add view
admin.add_view(MyModelView(models.User, db.session, name='用户管理'))
admin.add_view(TaskDefineView(models.TaskDefine, db.session, name='一般任务', category='任务配置'))
admin.add_view(CronConfView(models.CronConf, db.session, name='定时任务', category='任务配置'))
admin.add_view(TaskInstanceView(models.TaskInstance, db.session, name='任务实例', category='任务执行状况'))
admin.add_view(MyModelView(models.TaskDependency, db.session, name='任务依赖', category='任务配置'))
admin.add_view(CronView(models.CronLog, db.session, name='定时任务运行情况', category='任务执行状况'))
admin.add_view(TaskInstanceLogView(models.TtaskInstanceLog, db.session, name='任务执行日志', category='任务执行状况'))
admin.add_view(MyModelView(models.StatResult, db.session, name='运行状态', category='任务执行状况'))

# admin.add_view(rediscli.RedisCli(Redis()))


if __name__ == '__main__':
    # Build a sample db on the fly, if one does not exist yet.
    app_dir = os.path.realpath(os.path.dirname(__file__))
    print(generate_password_hash("test"))
    # database_path = os.path.join(app_dir, app.config['DATABASE_FILE'])
    # if not os.path.exists(database_path):
    # build_sample_db()

    # Start app
    app.run(debug=True)
