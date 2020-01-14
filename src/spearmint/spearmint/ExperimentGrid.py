##
# Copyright (C) 2012 Jasper Snoek, Hugo Larochelle and Ryan P. Adams
#
# This code is written for research and educational purposes only to
# supplement the paper entitled
# "Practical Bayesian Optimization of Machine Learning Algorithms"
# by Snoek, Larochelle and Adams
# Advances in Neural Information Processing Systems, 2012
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
import os
import sys
import tempfile
import cPickle
import random

import numpy        as np
import numpy.random as npr

from spearmint_pb2 import *
from Locker        import *
from sobol_lib     import *
from helpers       import *

CANDIDATE_STATE = 0
SUBMITTED_STATE = 1
RUNNING_STATE   = 2
COMPLETE_STATE  = 3
BROKEN_STATE    = -1

EXPERIMENT_GRID_FILE = 'expt-grid.pkl'

class ExperimentGrid:

    @staticmethod
    def job_running(expt_dir, id):
        expt_grid = ExperimentGrid(expt_dir)
        expt_grid.set_running(id)

    @staticmethod
    def job_complete(expt_dir, id, value, duration):
        log("setting job %d complete" % id)
        expt_grid = ExperimentGrid(expt_dir)
        expt_grid.set_complete(id, value, duration)
        log("set...")

    @staticmethod
    def job_broken(expt_dir, id):
        expt_grid = ExperimentGrid(expt_dir)
        expt_grid.set_broken(id)

    def __init__(self, expt_dir, variables=None, grid_size=None, grid_seed=1):
        self.expt_dir = expt_dir
        self.jobs_pkl = os.path.join(expt_dir, EXPERIMENT_GRID_FILE)
        self.locker   = Locker()

        # Only one process at a time is allowed to have access to the grid.
        self.locker.lock_wait(self.jobs_pkl)

        # Set up the grid for the first time if it doesn't exist.
        if variables is not None and not os.path.exists(self.jobs_pkl):
            self.seed     = random.randint(1, 10000)
            # self.seed = 3067
            self.vmap     = GridMap(variables, grid_size)
            self.grid     = self._hypercube_grid(self.vmap.card(), grid_size)
            self.status   = np.zeros(grid_size, dtype=int) + CANDIDATE_STATE
            self.values   = np.zeros(grid_size) + np.nan
            self.durs     = np.zeros(grid_size) + np.nan
            self.executed = np.zeros(grid_size) + 0
            self.proc_ids = np.zeros(grid_size, dtype=int)
            self._save_jobs()

        # Or load in the grid from the pickled file.
        else:
            self._load_jobs()


    def __del__(self):
        self._save_jobs()
        if self.locker.unlock(self.jobs_pkl):
            pass
        else:
            raise Exception("Could not release lock on job grid.\n")

    def get_grid(self):
        return self.grid, self.values, self.durs

    def get_candidates(self):
        return np.nonzero(self.status == CANDIDATE_STATE)[0]

    def get_pending(self):
        return np.nonzero((self.status == SUBMITTED_STATE) | (self.status == RUNNING_STATE))[0]

    def get_complete(self):
        return np.nonzero(self.status == COMPLETE_STATE)[0]

    def get_broken(self):
        return np.nonzero(self.status == BROKEN_STATE)[0]

    def get_executed(self):
        return np.nonzero(self.executed == 1)[0]

    def get_params(self, index):
        return self.vmap.get_params(self.grid[index,:])

    def get_best(self):
        finite = self.values[np.isfinite(self.values)]
        if len(finite) > 0:
            cur_min = np.min(finite)
            index   = np.nonzero(self.values==cur_min)[0][0]
            return cur_min, index
        else:
            return np.nan, -1

    def get_proc_id(self, id):
        return self.proc_ids[id]

    def add_to_grid(self, candidate, add_to_end=True):
        # Checks to prevent numerical over/underflow from corrupting the grid
        candidate[candidate > 1.0] = 1.0
        candidate[candidate < 0.0] = 0.0

        # Set up the grid
        if add_to_end:
            self.grid   = np.vstack((self.grid, candidate))
        else:
            self.grid = np.vstack((candidate,self.grid))

        self.status = np.append(self.status, np.zeros(1, dtype=int) +
                                int(CANDIDATE_STATE))

        self.values = np.append(self.values, np.zeros(1)+np.nan)
        self.durs   = np.append(self.durs, np.zeros(1)+np.nan)
        self.executed = np.append(self.executed, np.zeros(1) + np.nan)
        self.proc_ids = np.append(self.proc_ids, np.zeros(1,dtype=int))

        # Save this out.
        self._save_jobs()
        return self.grid.shape[0]-1

    def set_candidate(self, id):
        self.status[id] = CANDIDATE_STATE
        self._save_jobs()

    def set_submitted(self, id, proc_id):
        self.status[id] = SUBMITTED_STATE
        self.proc_ids[id] = proc_id
        self._save_jobs()

    def set_running(self, id):
        self.status[id] = RUNNING_STATE
        self._save_jobs()

    def set_complete(self, id, value, duration):
        self.status[id] = COMPLETE_STATE
        self.values[id] = value
        self.durs[id]   = duration
        self.executed[id] = 1
        self._save_jobs()

    def set_broken(self, id):
        self.status[id] = BROKEN_STATE
        self._save_jobs()

    def _load_jobs(self):
        fh   = open(self.jobs_pkl, 'r')
        jobs = cPickle.load(fh)
        fh.close()

        self.vmap   = jobs['vmap']
        self.grid   = jobs['grid']
        self.status = jobs['status']
        self.values = jobs['values']
        self.durs   = jobs['durs']
        self.executed = jobs['executed']
        self.proc_ids = jobs['proc_ids']

    def _save_jobs(self):

        # Write everything to a temporary file first.
        fh = tempfile.NamedTemporaryFile(mode='w', delete=False)
        cPickle.dump({ 'vmap'   : self.vmap,
                       'grid'   : self.grid,
                       'status' : self.status,
                       'values' : self.values,
                       'durs'   : self.durs,
                       'executed': self.executed,
                       'proc_ids' : self.proc_ids }, fh, protocol=-1)
        fh.close()

        # Use an atomic move for better NFS happiness.
        cmd = 'mv "%s" "%s"' % (fh.name, self.jobs_pkl)
        os.system(cmd) # TODO: Should check system-dependent return status.

    def check_constraints(self, all_constraints, constraints_satisfied, constraint_actual_sums, constraint_desired_sums,
                          constraint_estimates, row):
        for constraint_index in range(len(constraints_satisfied)):
            if constraints_satisfied[constraint_index]:
                continue

            constraint = all_constraints[constraint_index]
            sum = 0
            for ind in constraint:
                sum += row[ind]
            if sum > 1:
                constraint_actual_sums[constraint_index] = sum
                constraint_estimates[constraint_index] = sum / constraint_desired_sums[constraint_index]
            else:
                constraints_satisfied[constraint_index] = True


    def apply_constraint(self, all_constraints, constraint_actual_sums, constraint_desired_sums, index, row):
        current_sum = constraint_actual_sums[index]
        desired_sum = constraint_desired_sums[index]
        constraint = all_constraints[index]

        # print(row)
        for i in constraint:

            row[i] *= desired_sum / current_sum
        # print(row)

        sum = 0
        for j in constraint:
            sum += row[j]
        # print("old sum was {} and new sum is {} and constraint index is {} and row is {}".format(current_sum, sum, index, row))


    def _hypercube_grid(self, dims, size):

        np.random.seed(self.seed)

        # Generate from a sobol sequence
        # print("seed is {}".format(self.seed))

        # sobol_grid = np.transpose(i4_sobol_generate(dims,size,self.seed))


        array = np.random.uniform(0, 1, [size, dims])

        base_constraints = []
        base_constraints.append([3, 1, 0])
        base_constraints.append([1, 2, 0])
        base_constraints.append([1, 6, 4])
        base_constraints.append([1, 5, 0])

        all_constraints = []

        service_count = dims / 4

        for i in range(0, 4):
            for constraint in base_constraints:
                all_constraints.append([c + i * service_count for c in constraint])

        for row_index in range(len(array)):
            # print(row_index)
            row = array[row_index]

            for i in range(service_count):
                row[len(row) - 1 - i] /= 2

            constraints_satisfied = np.repeat(False, len(all_constraints))
            constraint_estimates = np.zeros(len(all_constraints))
            constraint_desired_sums = np.repeat(.95, len(all_constraints))
            constraint_actual_sums = np.zeros(len(all_constraints))

            while not np.all(constraints_satisfied):
                # print(constraints_satisfied)
                self.check_constraints(all_constraints, constraints_satisfied, constraint_actual_sums,
                                       constraint_desired_sums, constraint_estimates, row)

                next_index = None
                for index in range(len(constraints_satisfied)):
                    if not constraints_satisfied[index]:
                        next_index = index
                        break

                if next_index != None:
                    self.apply_constraint(all_constraints, constraint_actual_sums, constraint_desired_sums, next_index, row)

        print("Grid is generated")
        return array

class GridMap:

    def __init__(self, variables, grid_size):
        self.variables   = []
        self.cardinality = 0

        # Count the total number of dimensions and roll into new format.
        for variable in variables:
            self.cardinality += variable.size

            if variable.type == Experiment.ParameterSpec.INT:
                self.variables.append({ 'name' : variable.name,
                                        'size' : variable.size,
                                        'type' : 'int',
                                        'min'  : int(variable.min),
                                        'max'  : int(variable.max)})

            elif variable.type == Experiment.ParameterSpec.FLOAT:
                self.variables.append({ 'name' : variable.name,
                                        'size' : variable.size,
                                        'type' : 'float',
                                        'min'  : float(variable.min),
                                        'max'  : float(variable.max)})

            elif variable.type == Experiment.ParameterSpec.ENUM:
                self.variables.append({ 'name'    : variable.name,
                                        'size'    : variable.size,
                                        'type'    : 'enum',
                                        'options' : list(variable.options)})
            else:
                raise Exception("Unknown parameter type.")
        log("Optimizing over %d dimensions\n" % (self.cardinality))

    def get_params(self, u):
        if u.shape[0] != self.cardinality:
            raise Exception("Hypercube dimensionality is incorrect.")

        params = []
        index  = 0
        for variable in self.variables:
            param = Parameter()

            param.name = variable['name']

            if variable['type'] == 'int':
                for dd in xrange(variable['size']):
                    param.int_val.append(variable['min'] + self._index_map(u[index], variable['max']-variable['min']+1))
                    index += 1

            elif variable['type'] == 'float':
                for dd in xrange(variable['size']):
                    param.dbl_val.append(variable['min'] + u[index]*(variable['max']-variable['min']))
                    index += 1

            elif variable['type'] == 'enum':
                for dd in xrange(variable['size']):
                    ii = self._index_map(u[index], len(variable['options']))
                    index += 1
                    param.str_val.append(variable['options'][ii])

            else:
                raise Exception("Unknown parameter type.")

            params.append(param)

        return params

    def card(self):
        return self.cardinality

    def _index_map(self, u, items):
        u = np.max((u, 0.0))
        u = np.min((u, 1.0))
        return int(np.floor((1-np.finfo(float).eps) * u * float(items)))
