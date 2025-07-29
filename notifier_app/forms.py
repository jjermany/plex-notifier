from flask_wtf import FlaskForm
from wtforms import StringField, IntegerField, BooleanField, SubmitField
from wtforms.validators import DataRequired, URL, Optional, Email
from wtforms import EmailField
from wtforms import IntegerField
from wtforms.validators import NumberRange

class TestEmailForm(FlaskForm):
    test_email = EmailField(
        'Send a test email to',
        validators=[DataRequired(), Email()]
    )
    test_submit = SubmitField('Send Test Email')


class SettingsForm(FlaskForm):
    plex_url           = StringField('Plex URL', validators=[DataRequired(), URL()])
    plex_token         = StringField('Plex Token', validators=[DataRequired()])
    tautulli_url       = StringField('Tautulli URL', validators=[Optional(), URL()])
    tautulli_api_key   = StringField('Tautulli API Key', validators=[Optional()])
    base_url           = StringField('Base URL', validators=[Optional(), URL()])
    smtp_host          = StringField('SMTP Host', validators=[Optional()])
    smtp_port          = IntegerField('SMTP Port', validators=[Optional()])
    smtp_user          = StringField('SMTP Username', validators=[Optional()])
    smtp_pass          = StringField('SMTP Password', validators=[Optional()])
    from_address       = StringField('From Email', validators=[Optional(), Email()])
    notify_new_episodes= BooleanField('Notify on new episodes')
    submit             = SubmitField('Save Settings')
    notify_interval    = IntegerField(
        'Poll interval (minutes)',
        validators=[NumberRange(min=1, max=1440)],
        default=30,
        description='How often to check for new episodes.'
    )
