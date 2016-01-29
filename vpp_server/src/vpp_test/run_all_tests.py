import unittest

from vpp_test.data_acquisition.adapter.date_strategy_test import DefaultDateStrategyTest
from vpp_test.data_acquisition.data_processor_test import DefaultDataProcessorTest
from vpp_test.data_acquisition.interpreter import thor_interpreter_test
from vpp_test.data_acquisition.interpreter.energinet_co2_interpreter_test import EnerginetCO2InterpreterTest
from vpp_test.data_acquisition.interpreter.energinet_online_interpreter_test import EnerginetOnlineInterpreterTest
from vpp_test.data_acquisition.interpreter.grundfos_interpreter_test import GrundfosInterpreterTest
from vpp_test.data_acquisition.interpreter.nordpoolspot_interpreter_test import NordpoolspotInterpreterTest
from vpp_test.data_acquisition.interpreter.nrgi_abs_interpreter_test import NrgiAbsInterpreterTest
from vpp_test.data_acquisition.interpreter.nrgi_delta_interpreter_test import NrgiDeltaInterpreterTest
from vpp_test.data_acquisition.interpreter.smartamm_interpreter_test import SmartammInterpreterTest
from vpp_test.data_acquisition.interpreter.thor_interpreter_test import ThorInterpreterTest
from vpp_test.database.db_manager_test import DBManagerTest
from vpp_test.database.schema_manager_test import SchemaManagerTest


def run_all_tests():

    suite = unittest.TestSuite()

    suite.addTest(unittest.makeSuite(DefaultDateStrategyTest))

    suite.addTest(unittest.makeSuite(EnerginetCO2InterpreterTest))
    suite.addTest(unittest.makeSuite(EnerginetOnlineInterpreterTest))
    suite.addTest(unittest.makeSuite(GrundfosInterpreterTest))
    suite.addTest(unittest.makeSuite(NordpoolspotInterpreterTest))
    suite.addTest(unittest.makeSuite(NrgiAbsInterpreterTest))
    suite.addTest(unittest.makeSuite(NrgiDeltaInterpreterTest))
    suite.addTest(unittest.makeSuite(SmartammInterpreterTest))
    suite.addTest(unittest.makeSuite(ThorInterpreterTest))

    suite.addTest(unittest.makeSuite(DefaultDataProcessorTest))

    suite.addTest(unittest.makeSuite(DBManagerTest))
    suite.addTest(unittest.makeSuite(SchemaManagerTest))


    runner=unittest.TextTestRunner()
    runner.run(suite)


if __name__ == '__main__':
    run_all_tests()