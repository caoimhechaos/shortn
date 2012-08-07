opts = Variables( 'options.conf', ARGUMENTS )

opts.Add("DESTDIR",
	 'Set the root directory to install into (/path/to/DESTDIR)', "")
opts.Add("GOBIN",
	 'Set the directory to install binaries into (/path/to/GOBIN)',
	 "/usr/lib/go/bin")

env = Environment(ENV = {'GOROOT': '/usr/lib/go'}, TOOLS=['default', 'go'],
		  options = opts)

shortn = env.Go('shortn', ['db.go', 'shortn.go'])
bin = env.GoProgram('shortn', shortn)

env.Install(env['DESTDIR'] + env['ENV']['GOBIN'], bin)
env.Alias('install', [env['DESTDIR'] + env['ENV']['GOBIN']])

opts.Save('options.conf', env)
