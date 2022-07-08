import unittest
import cldfbench_clld_meta as m


class JSONExtraction(unittest.TestCase):

    def test_no_pre_tag(self):
        html = (
            '<html>\n'
            '<body>\n'
            '<p>no json</p>\n'
            '</body>\n'
            '</html>')
        with self.assertRaises(ValueError):
            json_data = m.extract_json(html)

    def test_good_pre_tag(self):
        html = (
            '<html>\n'
            '<body>\n'
            '<p>json</p>\n'
            '<pre style="white-space: pre-wrap;">{&#34;key&#34;: &#34;value&#34;}</pre>\n'
            '</body>\n'
            '</html>')
        json_data = m.extract_json(html)
        self.assertEqual(json_data, {'key': 'value'})

    def test_no_good_pre_tag(self):
        html = (
            '<html>\n'
            '<body>\n'
            '<p>no json</p>\n'
            '<pre>I am some other kind of code</pre>\n'
            '</body>\n'
            '</html>')
        with self.assertRaises(ValueError):
            json_data = m.extract_json(html)

    def test_good_and_non_good_pre_tag(self):
        html = (
            '<html>\n'
            '<body>\n'
            '<p>no json</p>\n'
            '<pre>I am some other kind of code</pre>\n'
            '<p>json</p>\n'
            '<pre style="white-space: pre-wrap;">{&#34;key&#34;: &#34;value&#34;}</pre>\n'
            '</body>\n'
            '</html>')
        json_data = m.extract_json(html)
        self.assertEqual(json_data, {'key': 'value'})

    def test_multiple_pre_tags(self):
        html = (
            '<html>\n'
            '<body>\n'
            '<p>json</p>\n'
            '<pre style="white-space: pre-wrap;">{&#34;key 1&#34;: &#34;value 1&#34;}</pre>\n'
            '<p>also json</p>\n'
            '<pre style="white-space: pre-wrap;">{&#34;key 2&#34;: &#34;value 2&#34;}</pre>\n'
            '</body>\n'
            '</html>')
        with self.assertRaises(ValueError):
            json_data = m.extract_json(html)


def test_valid(cldf_dataset, cldf_logger):
    assert cldf_dataset.validate(log=cldf_logger)
