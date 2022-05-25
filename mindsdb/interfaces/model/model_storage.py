import os

import mindsdb.interfaces.storage.db as db
from mindsdb.interfaces.storage.fs import FsStore
from mindsdb.utilities.config import Config


class ModelStorageError(Exception):
    pass


class ModelsStorage:

    def __init__(self, company_id):
        self.company_id = company_id

    def _get_models_query(self):
        query = db.session.query(
            db.Predictor.id,
            db.Predictor.name,
            db.Predictor.type,
        ).filter_by(
            db.Predictor.company_id == self.company_id,
            db.Predictor.status != 'deleted',
        )

        return query

    def list(self, model_type=None):
        models = []

        query = self._get_models_query()

        if model_type is not None:
            query = query.filter_by(type=model_type)

        for rec in query:
            model = ModelStore(rec.id)
            models.append(model)

        return models

    def get(self, name):

        query = self._get_models_query()
        query = query.filter(db.Predictor.name == name)

        rec = query.first()

        if rec is not None:
            return ModelStore(rec.id)

    def create(self, name, type, integration_id, target):
        if self.get(name) is not None:
            raise ModelStorageError('Name already exists')

        rec = db.Predictor(
            company_id=self.company_id,
            name=name,
            type=type,
            integration_id=integration_id,
            target=target
        )
        db.session.add(rec)
        db.session.commit()

        return ModelStore(rec.id)

    def delete(self, name):

        model = self.get(name)
        if model is None:
            raise ModelStorageError('Name is not exists')

        model.delete()


class ModelStore:

    def __init__(self, model_id):
        self.id = model_id
        rec = db.Predictor.query.get(model_id)

        # common properties
        self.name = rec.name
        self.type = rec.type
        self.status = rec.status

        self.target = rec.target
        self.integration_id = rec.integration_id  # how to use id ?

        # for file storage
        self._fs_store = FsStore()
        self._file_prefix = f'predictor_{model_id}_'
        config = Config()
        self._buffer_path = config['paths']['predictors']

    # -- columns --
    def get_columns(self):
        rec = db.Predictor.query.get(self.id)
        return rec.columns

    def set_columns(self, columns):
        rec = db.Predictor.query.get(self.id)
        # repack
        columns = [
            dict(
                name=i['name'],
                dtype=i['dtype']
            )
            for i in columns
        ]

        rec.columns = columns
        db.session.commit()

    # -- status --
    def get_status(self):
        rec = db.Predictor.query.get(self.id)
        return rec.status

    def set_status(self, status):
        allowed_statuses = ['generating', 'completed', 'training', 'error']

        if not status in allowed_statuses:
            raise ModelStorageError('Wrong status')

        rec = db.Predictor.query.get(self.id)
        rec.status = status
        db.session.commit()

    # -- progress --
    def get_progress(self):
        rec = db.Predictor.query.get(self.id)
        return rec.progress

    def set_progress(self, percentage):
        if percentage > 100 or percentage < 0:
            raise ModelStorageError('Wrong percentage')

        rec = db.Predictor.query.get(self.id)
        rec.progress = percentage
        db.session.commit()

    # -- custom properties for model --
    def prop_list(self):
        rec = db.Predictor.query.get(self.id)
        return list(rec.properties.keys())

    def prop_get(self, field):
        rec = db.Predictor.query.get(self.id)
        return rec.properties[field]

    def prop_set(self, field, value):
        rec = db.Predictor.query.get(self.id)
        properties = rec.properties.copy()

        properties[field] = value
        rec.properties = rec.properties
        db.session.commit()

    # -- model files --
    def file_list(self):
        return self._fs_store.list(self._file_prefix)

    def file_get(self, name):
        file_name = self._file_prefix + name
        self._fs_store.get(file_name, file_name, self._buffer_path)

        buf_name = os.path.join(self._buffer_path, file_name)
        return open(buf_name, 'rb')

    def file_set(self, name, content_or_fd):
        # TODO
        #  for local file - work with it directly from store
        #  for S3 file - store in temporary directory and remove after file is closed

        # detect type of content_or_fd
        if isinstance(content_or_fd, str):
            # str type
            content = content_or_fd.encode()

        elif isinstance(content_or_fd, bytes):
            # binary type
            content = content_or_fd
        else:
            # file descriptor
            content = content_or_fd.read()
            if 'b' not in content_or_fd.mode:
                # is text mode
                content = content.encode()

        # save before pass to fs_store
        file_name = self._file_prefix + name
        buf_name = os.path.join(self._buffer_path, file_name)
        with open(buf_name, 'wb') as fd:
            fd.write(content)

        self._fs_store.put(file_name, file_name, self._buffer_path)

    def file_del(self, name):
        file_name = self._file_prefix + name
        self._fs_store.delete(file_name)

        # remove buffered file
        buf_path = os.path.join(self._buffer_path, file_name)
        if os.path.exists(buf_path):
            os.unlink(buf_path)

    # -- model jsons --
    def json_list(self):
        names = []
        for rec in db.PredictorJson.query.filter_by(predictor_id=self.id):
            names.append(rec.name)

        return names

    def json_get(self, name):
        rec = db.PredictorJson.query.filter_by(predictor_id=self.id, name=name).first()
        if rec is None:
            raise ModelStorageError(f"Json doesn't exist: {name}")
        return rec.content

    def json_set(self, name, content):
        rec = db.PredictorJson.query.filter_by(predictor_id=self.id, name=name).first()
        if rec is None:
            rec = db.PredictorJson(
                predictor_id=self.id,
                name=name
            )
            db.session.add(rec)
            db.session.flush()

        rec.content = content
        db.session.commit()

    def json_del(self, name):
        db.PredictorJson.query\
            .filter_by(predictor_id=self.id, name=name)\
            .delete(synchronize_session=False)
        db.session.commit()

    def delete(self):
        # delete all artifacts but keep record
        for name in self.json_list():
            self.json_del(name)

        for name in self.file_list():
            self.file_del(name)

        # change status
        rec = db.Predictor.query.get(self.id)
        rec.status = 'deleted'
        db.session.commit()


'''
# can be passed to predictor_handler or wrapped
model_store = ModelsStorage(company_id=1)

model_store.list()

model = model_store.get('fish_model')

model.file_set('code', open('model.py'))
'''
