from setuptools import setup

setup(
    name = 'Marvin',
    version = '0.1.0',
    description = 'The MONROE scheduling client',
    author = 'Thomas Hirsch',
    author_email = 'thomas.hirsch@celerway.com',
    url = '',
    license = 'All rights reserved',
    packages = ['marvin'],
    entry_points = {'console_scripts': [
        'marvind    = marvin.marvind:main',
    ], },
    data_files = [
      ('/etc/', ['files/etc/marvind.conf']),
      ('/usr/bin/', ['files/usr/bin/container-stop.sh', 'files/usr/bin/container-start.sh', 'files/usr/bin/container-deploy.sh']),
      ('/lib/systemd/system/', ['files/lib/systemd/system/marvind.service']),
      ('/DEBIAN/', ['files/DEBIAN/postinst','files/DEBIAN/prerm']),
      ('/etc/cron.d/', ['files/etc/cron.d/marvind']),
    ],
    install_requires = [
      'requests', 'simplejson'
    ]
)
