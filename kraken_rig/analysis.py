import pandas as pd


class analysis():
    def __init__(self, trends):
        """
        -----------IMPLEMENT OTHER ANALYTICAL TOOLS!!-----------

        Intended to populate the analysis object with useful analysis tools...

        :param trends: The names and objects of trends that have been added
        :type trends: Dictionary
        """
        self.trends = {}
        self.frame_source = pd.DataFrame()
        for key in trends:
            if key not in self.__dict__.keys():
                setattr(self, key, trends[key])
            else:
                assert 'you somehow managed to add a trend twice, or try to overwrite some elemental attribute...'

    def step(self, cur_time):
        """
        Slot to fit logic of an analytical tool at each step.

        :param data: The data that is taken into consideration for the step.
        :type data: pandas.DataFrame
        """
        raise NotImplementedError

    # ease of use.
    def __getitem__(self, index):
        raise NotImplementedError
