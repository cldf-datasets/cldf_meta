from setuptools import setup


setup(
    name='cldfbench_clld_meta',
    py_modules=['cldfbench_clld_meta'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'cldfbench.dataset': ['clld_meta=cldfbench_clld_meta:Dataset'],
        'cldfbench.commands': ['clld-meta=clld_meta_commands'],
    },
    install_requires=[
        'cldfbench',
        'sickle',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
