<?php declare(strict_types=1);

/*
 * This file is part of the VV package.
 *
 * (c) Volodymyr Sarnytskyi <v00v4n@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */
namespace VV\Db\Oci;

use VV\Db\Param;

/**
 * Class Statement
 *
 * @package VV\Db\Oci
 */
class Statement implements \VV\Db\Driver\Statement
{

    private mixed $stmt;
    private mixed $ociConn;
    private ?array $lobsToUpload = [];
    private ?\VV\Db\Param $insertedIdParam = null;

    /**
     * Prepared constructor.
     *
     * @param mixed $stmt
     * @param mixed $ociConn
     */
    public function __construct(mixed $stmt, mixed $ociConn)
    {
        $this->stmt = $stmt;
        $this->ociConn = $ociConn;
    }

    /**
     * @inheritdoc
     */
    public function bind(array $params): void
    {
        /** @var Param[] $lobs */
        $lobs = []; // bind LOB params at the end
        if ($params) {
            $i = 0;
            foreach ($params as $k => &$param) {
                if ($param instanceof \VV\Db\Param && $param->isForInsertedId()) {
                    $this->insertedIdParam = $param;
                }

                if (is_string($k)) {
                    $name = ":$k";
                } elseif ($param instanceof Param && ($n = $param->getName())) {
                    $name = ":$n";
                } else {
                    $name = ':p' . (++$i);
                }

                if ($param instanceof Param) {
                    switch ($param->getType()) {
                        // LOBs to end
                        case Param::T_TEXT:
                        case Param::T_BLOB:
                            $lobs[$name] = $param;
                            break;

                        default:
                            $this->ociBindParam($name,
                                $param->getValue(),
                                $this->toOciTypeParamType($param),
                                $param->getSize() ?: -1
                            );
                    }
                    $param->setBinded();
                } else {
                    $this->ociBindParam($name, $param, $this->toOciTypeParamType($param));
                }
            }
            unset($param);
        }

        // bind deferred LOB params
        $this->lobsToUpload = [];
        foreach ($lobs as $name => $param) {
            $lobDescr = oci_new_descriptor($this->ociConn, OCI_D_LOB);
            if ($param->isForUpload()) {
                $this->lobsToUpload[] = [$lobDescr, $param];
            }

            $this->ociBindParam($name, $lobDescr, $this->toOciTypeParamType($param));
        }
    }

    /**
     * @inheritdoc
     */
    public function exec(): \VV\Db\Driver\Result
    {
        // execute query
        Driver::ociExecute($this->stmt);

        // upload LOBs
        /** @var \OCI_Lob $lobDescr */
        /** @var Param $param */
        foreach ($this->lobsToUpload as [$lobDescr, $param]) {
            if (!$value = $param->getValue()) {
                continue;
            }

            $lobDescr->rewind();
            foreach ($value as $block) {
                $lobDescr->write($block);
            }
        }

        $insertedId = $this->insertedIdParam ? $this->insertedIdParam->getValue() : null;

        return new Result($this->stmt, $insertedId);
    }

    /**
     * @inheritdoc
     */
    public function setFetchSize(int $size): void
    {
        oci_set_prefetch($this->stmt, $size);
    }

    /**
     * @inheritdoc
     */
    public function close(): void
    {
        oci_free_statement($this->stmt);
        $this->stmt = null;

        foreach ($this->lobsToUpload as [$lobDescr]) {
            /** @var \OCI_Lob $lobDescr */
            $lobDescr->close();
        }
        $this->lobsToUpload = [];
    }

    /**
     * @param     $name
     * @param     $value
     * @param     $type
     * @param int $size
     */
    private function ociBindParam($name, &$value, $type, $size = -1)
    {
        $ob = oci_bind_by_name($this->stmt, $name, $value, $size, $type);

        if (!$ob) {
            throw new \RuntimeException(
                'Bind params error: ' . $name . ' ' . $value,
                null,
                Driver::ociError($this->stmt)
            );
        }
    }

    /**
     * @param mixed $value
     *
     * @return int
     */
    private function toOciTypeParamType(mixed $value): int {
        $paramType = null;
        if ($value instanceof Param) {
            $paramType = $value->getType();
            $value = $value->getValue();
        }

        if ($paramType)
            switch ($paramType) {
                case Param::T_INT:
                    return SQLT_INT;

                case Param::T_TEXT:
                    return SQLT_CLOB;

                case Param::T_BLOB:
                    return SQLT_BLOB;

                case Param::T_BIN:
                    return SQLT_BIN;
            }

        if (is_int($value)) {
            return SQLT_INT;
        }

        return SQLT_CHR;
    }
}
