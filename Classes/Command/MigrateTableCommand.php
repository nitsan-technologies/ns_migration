<?php

namespace NITSAN\NsMigration\Command;

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Exception;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use TYPO3\CMS\Core\Database\ConnectionPool;
use TYPO3\CMS\Core\Database\Query\QueryBuilder;
use TYPO3\CMS\Core\Database\Query\Restriction\DeletedRestriction;
use TYPO3\CMS\Core\Utility\GeneralUtility;

class MigrateTableCommand extends Command
{
    const EXTERNAL_DATABASE = 'External Database Configured Name';

    const MIGRATE_TABLE = 'Migrate Table';

    const MIGRATE_TABLE_MM = 'Migrate MM Table';

    const RECORD_PID = 'Storage PID';

    const MIGRATE_RECORD_PID = 'Migrate to this Storage PID';

    /**
     * Configure the command by defining the name, options and arguments
     */
    protected function configure()
    {
        $this->setDescription('Copying the records from the old database to the new like migrations');
        $this
            ->addArgument(
                self::EXTERNAL_DATABASE,
                InputArgument::REQUIRED,
                'External Database name which you have configured in LocalConfiguration/AdditionalConfiguration.php file.'
            )
            ->addArgument(
                self::MIGRATE_TABLE,
                InputArgument::REQUIRED,
                'Add Table name'
            )
            ->addArgument(
                self::RECORD_PID,
                InputArgument::REQUIRED,
                'Record storage PID'
            )
            ->addArgument(
                self::MIGRATE_RECORD_PID,
                InputArgument::REQUIRED,
                'Migrate to storage PID'
            )
            ->addArgument(
                self::MIGRATE_TABLE_MM,
                InputArgument::OPTIONAL,
                'Add Migrate Table name'
            );
    }

    /**
     * @param InputInterface $input
     * @param OutputInterface $output
     * @return int
     * @throws \Doctrine\DBAL\Driver\Exception|DBALException
     */
    public function execute(InputInterface $input, OutputInterface $output): int
    {
        $inputs = $input->getArguments();
        if (!$inputs[self::MIGRATE_TABLE_MM]) {
           try {
               $this->import($inputs);
           } catch (Exception $e) {
               \TYPO3\CMS\Extbase\Utility\DebuggerUtility::var_dump($e->getMessage(), __FILE__.' Line No. '.__LINE__);die;
           }
       }
       return 0;
    }

    /**
     * @throws Exception
     * @throws \Doctrine\DBAL\Driver\Exception|DBALException
     *
     */
    private function import($inputs){
        $query = $this->getQueryBuilder($inputs[self::MIGRATE_TABLE]);
        $queryTableExternal = $this->getExternalConnection($inputs[self::EXTERNAL_DATABASE]);
        $queryTableExternal->getRestrictions()->removeAll()->add(GeneralUtility::makeInstance(DeletedRestriction::class));
        $data = $this->getDataForMigration($queryTableExternal, $inputs);
        if ($data) {
            foreach ($data as $record) {
                //Fetch the record from the database...
                $queryTableExternal = $this->getExternalConnection($inputs[self::EXTERNAL_DATABASE]);
                $sysFileRefOldRecords = $queryTableExternal->select('*')
                    ->from('sys_file_reference')
                    ->where(
                        $queryTableExternal->expr()->eq('uid_foreign', $record['uid'])
                    )
                    ->execute()->fetchAllAssociative();
                $record['pid'] = (int)$inputs[self::MIGRATE_RECORD_PID];
                $query->insert($inputs[self::MIGRATE_TABLE])
                    ->values($record)
                    ->execute();
                //Checking the sys_file_reference records...
                if ($sysFileRefOldRecords) {
                    $queryTableExternal = $this->getExternalConnection($inputs[self::EXTERNAL_DATABASE]);
                    $query = $this->getQueryBuilder('sys_file_reference');
                    $query->delete('sys_file_reference')
                        ->where(
                            $query->expr()->eq('tablenames', $query->createNamedParameter($inputs[self::MIGRATE_TABLE])),
                            $query->expr()->eq('uid_foreign', $record['uid']),
                        )
                        ->execute();
                    foreach ($sysFileRefOldRecords as $sysFileRecord) {
                        $sysIdentifier = $queryTableExternal->select('identifier')
                            ->from('sys_file')
                            ->where(
                                $queryTableExternal->expr()->eq('uid', $sysFileRecord['uid_local'])
                            )
                            ->execute()->fetchOne();
                        $query = $this->getQueryBuilder('sys_file');
                        $sysFileUid = $query->select('uid')
                            ->from('sys_file')
                            ->where(
                                $query->expr()->eq('identifier', "'".$sysIdentifier."'")
                            )
                            ->execute()
                            ->fetchOne();
                        if ($sysFileUid) {
                            //Manipulation of the file reference records...
                            $recordForSysFileReference = [
                                'pid' => $inputs[self::MIGRATE_RECORD_PID],
                                'uid_local' => $sysFileUid,
                                'uid_foreign' => $record['uid'],
                                'tablenames' => $inputs[self::MIGRATE_TABLE],
                                'table_local' => 'sys_file',
                                'fieldname' => $sysFileRecord['fieldname']
                            ];

                            $query = $this->getQueryBuilder('sys_file_reference');
                            $query->insert('sys_file_reference')->values($recordForSysFileReference)->execute();
                        }
                    }
                }
            }
        }
    }

    /**
     * @throws Exception|DBALException
     */
    private function importWithMM($inputs){
        $query = $this->getQueryBuilder($inputs);
        $queryTableExternal = $this->getExternalConnection($inputs[self::EXTERNAL_DATABASE]);
        $queryTableExternal->getRestrictions()->removeAll()->add(GeneralUtility::makeInstance(DeletedRestriction::class));
        $data = $this->getDataForMigration($queryTableExternal, $inputs);
        if ($data) {
            foreach ($data as $record) {
                $query->insert($inputs[self::MIGRATE_TABLE])
                    ->values($record)
                    ->execute();
            }
        }
    }

    /**
     * @param string $connection
     * @return QueryBuilder
     * @throws Exception
     */
    private function getExternalConnection(string $connection): QueryBuilder
    {
        return GeneralUtility::makeInstance(ConnectionPool::class)
            ->getConnectionByName($connection)
            ->createQueryBuilder();
    }

    /**
     * @param $table
     * @return QueryBuilder
     */
    private function getQueryBuilder($table): QueryBuilder
    {
        return GeneralUtility::makeInstance(ConnectionPool::class)
            ->getQueryBuilderForTable($table);
    }

    /**
     * @param $queryTableExternal
     * @param $inputs
     * @return mixed
     */
    private function getDataForMigration($queryTableExternal, $inputs) {
        return
            $queryTableExternal->select('*')
            ->from($inputs[self::MIGRATE_TABLE])
            ->where(
                $queryTableExternal->expr()->eq('pid', $inputs[self::RECORD_PID])
            )
            ->execute()
            ->fetchAllAssociative();
    }
}
