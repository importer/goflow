from functools import wraps
def cmd(func):
# cmd=args.get('cmd')
# setattr(func,'delay',lambda *args, **kwargs:self._publish_cmdtask(uuid.uuid4().hex, cmd, *args, **kwargs))
	@wraps(func)
	def _w(*args, **kwargs):
		if len(args):
			cmd=args[0]
		print(cmd)
		return func(*args, **kwargs)
	return _w

@cmd
def cmd_demo(cmd='help'):
    cmd='python -h'

cmd_demo('python -h')
